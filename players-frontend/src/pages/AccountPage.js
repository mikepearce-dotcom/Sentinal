import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Link, Navigate } from 'react-router-dom';
import api from '../api/axios';
import { GhostButton, PrimaryButton, Tag } from '../components/UI';
import { useAuth } from '../hooks/useAuth';

const getErrorMessage = (err, fallback) => {
  const detail = err?.response?.data?.detail;
  if (Array.isArray(detail)) return detail.map((d) => d?.msg || 'Invalid input').join(', ');
  if (typeof detail === 'string' && detail.trim()) return detail;
  return err?.message || fallback;
};

const readFileAsDataUrl = (file) =>
  new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ''));
    reader.onerror = () => reject(new Error('Could not read file'));
    reader.readAsDataURL(file);
  });

const loadImage = (src) =>
  new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = () => reject(new Error('Could not load image'));
    img.src = src;
  });

async function buildAvatarDataUrl(file) {
  if (!file || !file.type || !file.type.startsWith('image/')) {
    throw new Error('Please choose an image file.');
  }
  if (file.size > 6 * 1024 * 1024) {
    throw new Error('Image is too large. Please use an image under 6MB.');
  }

  const sourceDataUrl = await readFileAsDataUrl(file);
  const image = await loadImage(sourceDataUrl);

  const outputSize = 320;
  const canvas = document.createElement('canvas');
  canvas.width = outputSize;
  canvas.height = outputSize;
  const ctx = canvas.getContext('2d');
  if (!ctx) throw new Error('Could not process image.');

  const sourceW = image.naturalWidth || image.width;
  const sourceH = image.naturalHeight || image.height;
  const side = Math.min(sourceW, sourceH);
  const sx = Math.floor((sourceW - side) / 2);
  const sy = Math.floor((sourceH - side) / 2);

  ctx.clearRect(0, 0, outputSize, outputSize);
  ctx.drawImage(image, sx, sy, side, side, 0, 0, outputSize, outputSize);

  let dataUrl = canvas.toDataURL('image/webp', 0.88);
  if (!dataUrl || dataUrl === 'data:,') {
    dataUrl = canvas.toDataURL('image/jpeg', 0.86);
  }
  if (!dataUrl || dataUrl === 'data:,') {
    throw new Error('Could not generate avatar image.');
  }
  if (dataUrl.length > 390000) {
    dataUrl = canvas.toDataURL('image/jpeg', 0.72);
  }
  if (dataUrl.length > 390000) {
    throw new Error('Image is too large after processing. Try a smaller image.');
  }
  return dataUrl;
}

export default function AccountPage() {
  const { user, logout, refreshUser } = useAuth();
  const fileInputRef = useRef(null);

  const [account, setAccount] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [savingProfile, setSavingProfile] = useState(false);
  const [sendingReset, setSendingReset] = useState(false);
  const [avatarLoadFailed, setAvatarLoadFailed] = useState(false);
  const [processingUpload, setProcessingUpload] = useState(false);
  const [avatarMode, setAvatarMode] = useState('provider');
  const [form, setForm] = useState({ name: '', avatar_url: '' });

  const applyAccount = (next) => {
    setAccount(next || null);
    setAvatarLoadFailed(false);
    setForm({
      name: String(next?.name || ''),
      avatar_url: String(next?.custom_avatar_url || ''),
    });
    setAvatarMode(next?.custom_avatar_url ? 'custom' : 'provider');
  };

  useEffect(() => {
    let cancelled = false;

    const loadAccount = async () => {
      setLoading(true);
      setError('');
      try {
        const resp = await api.get('/api/auth/account');
        if (!cancelled) applyAccount(resp?.data || null);
      } catch (err) {
        if (!cancelled) setError(getErrorMessage(err, 'Failed to load account details.'));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    loadAccount();
    return () => {
      cancelled = true;
    };
  }, []);

  if (!user && !loading) return <Navigate to="/login" replace />;

  const providerPhoto = String(account?.auth0_picture_url || '');
  const customPhoto = String(form.avatar_url || '').trim();
  const effectivePreview = avatarMode === 'custom' ? (customPhoto || providerPhoto) : providerPhoto;
  const usingCustomAvatar = avatarMode === 'custom' && !!customPhoto;

  const avatarInitial = useMemo(() => {
    const source = String(form.name || account?.name || account?.email || user?.name || user?.email || 'U').trim();
    return source ? source.charAt(0).toUpperCase() : 'U';
  }, [account?.email, account?.name, form.name, user?.email, user?.name]);

  const onSaveProfile = async (event) => {
    event.preventDefault();
    setError('');
    setMessage('');
    setSavingProfile(true);
    try {
      const payload = {
        name: String(form.name || '').trim(),
        avatar_url: avatarMode === 'custom' ? String(form.avatar_url || '').trim() : '',
      };
      const resp = await api.patch('/api/auth/account/profile', payload);
      applyAccount(resp?.data || null);
      await refreshUser();
      setMessage('Profile updated.');
    } catch (err) {
      setError(getErrorMessage(err, 'Could not save profile changes.'));
    } finally {
      setSavingProfile(false);
    }
  };

  const onChooseFile = async (event) => {
    const file = event?.target?.files?.[0];
    if (!file) return;
    setError('');
    setMessage('');
    setProcessingUpload(true);
    try {
      const dataUrl = await buildAvatarDataUrl(file);
      setForm((prev) => ({ ...prev, avatar_url: dataUrl }));
      setAvatarMode('custom');
      setAvatarLoadFailed(false);
      setMessage('Avatar image ready. Save profile to apply it.');
    } catch (err) {
      setError(err?.message || 'Could not process image.');
    } finally {
      setProcessingUpload(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  const onSendPasswordReset = async () => {
    setError('');
    setMessage('');
    setSendingReset(true);
    try {
      const resp = await api.post('/api/auth/password-reset');
      setMessage(resp?.data?.message || 'Password reset email sent.');
    } catch (err) {
      setError(getErrorMessage(err, 'Could not start password reset.'));
    } finally {
      setSendingReset(false);
    }
  };

  return (
    <main className="page-shell max-w-5xl">
      <section className="card-glass p-6 md:p-8">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div>
            <p className="section-eyebrow">Account</p>
            <h1 className="panel-title text-3xl md:text-4xl font-bold mt-2">Profile & account settings</h1>
            <p className="mt-2 text-slate-500 text-sm">
              Manage your display name, profile photo, and security settings for Sentient Community.
            </p>
          </div>
          <div className="flex items-center gap-2 flex-wrap">
            <Link to="/petitions" className="btn-secondary px-4 py-2 text-sm font-semibold">Back to petitions</Link>
            <GhostButton onClick={logout} className="text-sm">Logout</GhostButton>
          </div>
        </div>

        {loading ? <p className="mt-5 text-slate-500">Loading account...</p> : null}

        {!loading && account ? (
          <div className="mt-6 grid grid-cols-1 xl:grid-cols-[0.95fr_1.05fr] gap-5">
            <section className="card-subtle p-5">
              <p className="text-xs uppercase tracking-[0.12em] text-slate-500 font-semibold">Profile photo</p>
              <div className="mt-4 flex items-start gap-4">
                <div className="w-24 h-24 rounded-2xl border border-slate-200 bg-white overflow-hidden shadow-sm flex items-center justify-center text-3xl font-bold text-sky-700">
                  {effectivePreview && !avatarLoadFailed ? (
                    <img
                      src={effectivePreview}
                      alt={form.name || account.name || 'User avatar'}
                      className="w-full h-full object-cover"
                      onError={() => setAvatarLoadFailed(true)}
                    />
                  ) : (
                    <span>{avatarInitial}</span>
                  )}
                </div>
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-semibold text-slate-900">{form.name || account.name || 'Community User'}</p>
                  <p className="text-xs text-slate-500 mt-1">
                    {usingCustomAvatar ? 'Using a custom profile picture' : providerPhoto ? 'Using your social/provider picture' : 'No provider picture found'}
                  </p>
                  <div className="mt-3 flex gap-2 flex-wrap">
                    <button
                      type="button"
                      className={`text-xs px-3 py-1.5 rounded-full border ${
                        avatarMode === 'provider'
                          ? 'border-sky-200 bg-sky-50 text-sky-700'
                          : 'border-slate-200 text-slate-600 bg-white'
                      }`}
                      onClick={() => {
                        setAvatarMode('provider');
                        setAvatarLoadFailed(false);
                      }}
                    >
                      Use social photo
                    </button>
                    <button
                      type="button"
                      className={`text-xs px-3 py-1.5 rounded-full border ${
                        avatarMode === 'custom'
                          ? 'border-teal-200 bg-teal-50 text-teal-700'
                          : 'border-slate-200 text-slate-600 bg-white'
                      }`}
                      onClick={() => {
                        setAvatarMode('custom');
                        setAvatarLoadFailed(false);
                      }}
                    >
                      Use custom photo
                    </button>
                  </div>
                </div>
              </div>

              <div className="mt-5 space-y-3">
                <div>
                  <label className="field-label">Upload image</label>
                  <div className="flex items-center gap-2 flex-wrap">
                    <input
                      ref={fileInputRef}
                      type="file"
                      accept="image/png,image/jpeg,image/webp,image/gif"
                      className="hidden"
                      onChange={onChooseFile}
                    />
                    <button
                      type="button"
                      onClick={() => fileInputRef.current?.click()}
                      disabled={processingUpload}
                      className="btn-secondary px-4 py-2 text-sm font-semibold disabled:opacity-60"
                    >
                      {processingUpload ? 'Processing image…' : 'Upload from device'}
                    </button>
                    <span className="text-xs text-slate-500">We crop and compress to a square avatar before saving.</span>
                  </div>
                </div>

                <div>
                  <label className="field-label">Or paste image URL</label>
                  <input
                    type="url"
                    value={form.avatar_url}
                    onChange={(event) => {
                      setForm((prev) => ({ ...prev, avatar_url: event.target.value }));
                      setAvatarMode('custom');
                      setAvatarLoadFailed(false);
                    }}
                    className="field-input"
                    placeholder="https://example.com/avatar.png"
                  />
                  <div className="mt-2 flex items-center gap-2 flex-wrap">
                    <button
                      type="button"
                      className="text-xs copy-link"
                      onClick={() => {
                        setForm((prev) => ({ ...prev, avatar_url: '' }));
                        setAvatarMode('provider');
                        setAvatarLoadFailed(false);
                      }}
                    >
                      Remove custom photo and use social photo
                    </button>
                    <Tag>{usingCustomAvatar ? 'Custom photo selected' : 'Provider photo selected'}</Tag>
                  </div>
                </div>
              </div>
            </section>

            <section className="card-subtle p-5">
              <form onSubmit={onSaveProfile} className="space-y-4">
                <div>
                  <label className="field-label">Display name</label>
                  <input
                    type="text"
                    value={form.name}
                    onChange={(event) => setForm((prev) => ({ ...prev, name: event.target.value }))}
                    className="field-input"
                    placeholder="Your display name"
                    maxLength={80}
                  />
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <div className="stat-tile">
                    <p className="stat-label">Email</p>
                    <p className="mt-1 text-sm text-slate-800 break-all">{account.email || 'Unknown'}</p>
                  </div>
                  <div className="stat-tile">
                    <p className="stat-label">Provider</p>
                    <p className="mt-1 text-sm text-slate-800">{account.provider || account.auth_provider || 'Unknown'}</p>
                  </div>
                  <div className="stat-tile md:col-span-2">
                    <p className="stat-label">User ID</p>
                    <p className="mt-1 text-xs text-slate-700 font-mono break-all">{account.user_id || 'Unknown'}</p>
                  </div>
                </div>

                <div className="flex justify-end">
                  <PrimaryButton type="submit" disabled={savingProfile || processingUpload} className="text-sm">
                    {savingProfile ? 'Saving profile…' : 'Save profile'}
                  </PrimaryButton>
                </div>
              </form>

              <div className="mt-6 pt-5 border-t border-slate-200">
                <h2 className="panel-title text-xl font-bold">Security</h2>
                <p className="mt-2 text-sm text-slate-500">
                  Password reset is available for email/password users. Social logins reset passwords through the identity provider.
                </p>

                {account.can_reset_password ? (
                  <button
                    type="button"
                    onClick={onSendPasswordReset}
                    disabled={sendingReset}
                    className="btn-secondary mt-4 px-4 py-2 text-sm font-semibold disabled:opacity-60"
                  >
                    {sendingReset ? 'Sending reset email…' : 'Send password reset email'}
                  </button>
                ) : (
                  <p className="mt-4 text-sm text-slate-500">No password reset available for this provider.</p>
                )}
              </div>
            </section>
          </div>
        ) : null}

        {error ? (
          <p className="mt-5 text-sm text-rose-700 bg-rose-50 border border-rose-100 rounded-xl px-3 py-2">{error}</p>
        ) : null}
        {message ? (
          <p className="mt-3 text-sm text-emerald-700 bg-emerald-50 border border-emerald-100 rounded-xl px-3 py-2">{message}</p>
        ) : null}
      </section>
    </main>
  );
}
