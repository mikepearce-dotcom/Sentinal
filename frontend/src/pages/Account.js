import React, { useContext, useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { AuthContext } from '../context/AuthContext';
import api from '../api/axios';

const getErrorMessage = (err, fallback) => {
  const detail = err?.response?.data?.detail;

  if (Array.isArray(detail)) {
    return detail.map((item) => item?.msg || 'Invalid input').join(', ');
  }

  if (typeof detail === 'string' && detail.trim()) {
    return detail;
  }

  return err?.message || fallback;
};

const Account = () => {
  const { logout } = useContext(AuthContext);

  const [account, setAccount] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [sendingReset, setSendingReset] = useState(false);
  const [savingProfile, setSavingProfile] = useState(false);
  const [avatarLoadFailed, setAvatarLoadFailed] = useState(false);
  const [profileForm, setProfileForm] = useState({ name: '', avatar_url: '' });

  const applyAccountState = (next) => {
    setAccount(next || null);
    setAvatarLoadFailed(false);
    setProfileForm({
      name: String(next?.name || ''),
      avatar_url: String(next?.custom_avatar_url || ''),
    });
  };

  useEffect(() => {
    let cancelled = false;

    const loadAccount = async () => {
      setLoading(true);
      setError('');

      try {
        const resp = await api.get('/api/auth/account');
        if (!cancelled) {
          applyAccountState(resp?.data || null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(getErrorMessage(err, 'Failed to load account details.'));
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    loadAccount();

    return () => {
      cancelled = true;
    };
  }, []);

  const handlePasswordReset = async () => {
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

  const handleProfileSave = async (event) => {
    event.preventDefault();
    setError('');
    setMessage('');
    setSavingProfile(true);

    try {
      const payload = {
        name: profileForm.name.trim(),
        avatar_url: profileForm.avatar_url.trim(),
      };
      const resp = await api.patch('/api/auth/account/profile', payload);
      applyAccountState(resp?.data || null);
      setMessage('Profile updated.');
    } catch (err) {
      setError(getErrorMessage(err, 'Could not save profile changes.'));
    } finally {
      setSavingProfile(false);
    }
  };

  const avatarInitial = useMemo(() => {
    const source = String(account?.name || account?.email || 'U').trim();
    return source ? source.charAt(0).toUpperCase() : 'U';
  }, [account?.email, account?.name]);

  const usingCustomAvatar = Boolean(account?.custom_avatar_url);

  return (
    <div className="min-h-screen bg-[#09090b]">
      <header className="border-b border-white/5">
        <div className="max-w-4xl mx-auto px-6 md:px-10 py-4 flex items-center justify-between gap-3">
          <div>
            <p className="font-heading font-black tracking-wide">ACCOUNT SETTINGS</p>
            <p className="text-xs text-zinc-500">Manage your profile and security.</p>
          </div>

          <div className="flex items-center gap-2">
            <Link
              to="/app"
              className="px-3 py-2 border border-white/15 text-sm text-zinc-300 hover:text-white hover:border-white/30"
            >
              Back to Dashboard
            </Link>
            <button
              type="button"
              onClick={logout}
              className="px-3 py-2 border border-white/15 text-sm text-zinc-300 hover:text-white hover:border-white/30"
            >
              Logout
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-4xl mx-auto px-6 md:px-10 py-8 space-y-6">
        <section className="card-glass p-6">
          <h1 className="font-heading text-3xl font-black">Profile</h1>

          {loading ? <p className="text-zinc-400 mt-4">Loading account...</p> : null}

          {!loading && account ? (
            <>
              <div className="mt-4 flex flex-col md:flex-row md:items-center gap-4 p-4 bg-black/30 border border-white/10">
                <div className="w-20 h-20 rounded-full bg-zinc-800 border border-white/15 overflow-hidden flex items-center justify-center text-2xl font-bold text-[#D3F34B]">
                  {account.avatar_url && !avatarLoadFailed ? (
                    <img
                      src={account.avatar_url}
                      alt={account.name || 'User avatar'}
                      className="w-full h-full object-cover"
                      onError={() => setAvatarLoadFailed(true)}
                    />
                  ) : (
                    <span>{avatarInitial}</span>
                  )}
                </div>
                <div>
                  <p className="text-zinc-100 font-semibold">{account.name || 'Unknown'}</p>
                  <p className="text-zinc-400 text-sm">{usingCustomAvatar ? 'Custom avatar enabled' : 'Using provider avatar'}</p>
                </div>
              </div>

              <form onSubmit={handleProfileSave} className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                <div className="p-4 bg-black/30 border border-white/10">
                  <p className="text-zinc-500 text-xs uppercase tracking-[0.12em]">Display Name</p>
                  <input
                    type="text"
                    value={profileForm.name}
                    onChange={(event) => setProfileForm((prev) => ({ ...prev, name: event.target.value }))}
                    className="mt-2 w-full px-3 py-2 bg-black/40 border border-white/10 text-zinc-100 focus:outline-none focus:border-[#D3F34B]/60"
                    placeholder="Your display name"
                    maxLength={80}
                  />
                </div>

                <div className="p-4 bg-black/30 border border-white/10">
                  <p className="text-zinc-500 text-xs uppercase tracking-[0.12em]">Custom Avatar URL</p>
                  <input
                    type="url"
                    value={profileForm.avatar_url}
                    onChange={(event) => setProfileForm((prev) => ({ ...prev, avatar_url: event.target.value }))}
                    className="mt-2 w-full px-3 py-2 bg-black/40 border border-white/10 text-zinc-100 focus:outline-none focus:border-[#D3F34B]/60"
                    placeholder="https://..."
                  />
                  <p className="text-xs text-zinc-500 mt-2">Leave empty to use your provider photo.</p>
                </div>

                <div className="md:col-span-2 flex justify-end">
                  <button
                    type="submit"
                    disabled={savingProfile}
                    className="px-4 py-2 border border-[#D3F34B]/40 text-[#e7ff8b] hover:text-white hover:border-[#D3F34B]/70 disabled:opacity-60"
                  >
                    {savingProfile ? 'SAVING PROFILE...' : 'SAVE PROFILE'}
                  </button>
                </div>
              </form>

              <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                <div className="p-4 bg-black/30 border border-white/10">
                  <p className="text-zinc-500 text-xs uppercase tracking-[0.12em]">Email</p>
                  <p className="text-zinc-100 mt-1">{account.email || 'Unknown'}</p>
                </div>
                <div className="p-4 bg-black/30 border border-white/10">
                  <p className="text-zinc-500 text-xs uppercase tracking-[0.12em]">Provider</p>
                  <p className="text-zinc-100 mt-1">{account.provider || account.auth_provider || 'Unknown'}</p>
                </div>
                <div className="p-4 bg-black/30 border border-white/10 md:col-span-2">
                  <p className="text-zinc-500 text-xs uppercase tracking-[0.12em]">User ID</p>
                  <p className="text-zinc-100 mt-1 font-mono break-all">{account.user_id || 'Unknown'}</p>
                </div>
              </div>
            </>
          ) : null}

          {error ? <p className="text-red-400 text-sm mt-4">{error}</p> : null}
          {message ? <p className="text-emerald-300 text-sm mt-4">{message}</p> : null}
        </section>

        <section className="card-glass p-6">
          <h2 className="font-heading text-2xl font-bold">Security</h2>
          <p className="text-zinc-400 text-sm mt-2">
            Reset password is available for Auth0 database users. Social logins reset via the identity provider.
          </p>

          {account?.can_reset_password ? (
            <button
              type="button"
              onClick={handlePasswordReset}
              disabled={sendingReset}
              className="mt-4 px-4 py-2 border border-[#D3F34B]/40 text-[#e7ff8b] hover:text-white hover:border-[#D3F34B]/70 disabled:opacity-60"
            >
              {sendingReset ? 'SENDING RESET EMAIL...' : 'SEND PASSWORD RESET EMAIL'}
            </button>
          ) : (
            <p className="text-zinc-500 text-sm mt-4">No password reset available for this provider.</p>
          )}
        </section>
      </main>
    </div>
  );
};

export default Account;
