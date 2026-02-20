import React, { useContext, useEffect, useState } from 'react';
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
  const [deleting, setDeleting] = useState(false);
  const [confirmText, setConfirmText] = useState('');

  useEffect(() => {
    let cancelled = false;

    const loadAccount = async () => {
      setLoading(true);
      setError('');

      try {
        const resp = await api.get('/api/auth/account');
        if (!cancelled) {
          setAccount(resp?.data || null);
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

  const handleDeleteAccount = async () => {
    setError('');
    setMessage('');

    if (confirmText.trim().toUpperCase() !== 'DELETE') {
      setError('Type DELETE to confirm account deletion.');
      return;
    }

    const confirmed = window.confirm('Delete your account and all game scan data? This cannot be undone.');
    if (!confirmed) {
      return;
    }

    setDeleting(true);
    try {
      const resp = await api.delete('/api/auth/account');
      const requiresManualDelete = Boolean(resp?.data?.requires_auth0_manual_delete);
      if (requiresManualDelete) {
        window.alert(
          'Your app data was deleted, but your Auth0 identity was not deleted automatically. You can remove it in Auth0 Dashboard > User Management > Users.'
        );
      }
      logout();
    } catch (err) {
      setError(getErrorMessage(err, 'Failed to delete account.'));
      setDeleting(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#09090b]">
      <header className="border-b border-white/5">
        <div className="max-w-4xl mx-auto px-6 md:px-10 py-4 flex items-center justify-between gap-3">
          <div>
            <p className="font-heading font-black tracking-wide">ACCOUNT SETTINGS</p>
            <p className="text-xs text-zinc-500">Manage your profile, security, and account lifecycle.</p>
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
            <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
              <div className="p-4 bg-black/30 border border-white/10">
                <p className="text-zinc-500 text-xs uppercase tracking-[0.12em]">Name</p>
                <p className="text-zinc-100 mt-1">{account.name || 'Unknown'}</p>
              </div>
              <div className="p-4 bg-black/30 border border-white/10">
                <p className="text-zinc-500 text-xs uppercase tracking-[0.12em]">Email</p>
                <p className="text-zinc-100 mt-1">{account.email || 'Unknown'}</p>
              </div>
              <div className="p-4 bg-black/30 border border-white/10">
                <p className="text-zinc-500 text-xs uppercase tracking-[0.12em]">Provider</p>
                <p className="text-zinc-100 mt-1">{account.provider || account.auth_provider || 'Unknown'}</p>
              </div>
              <div className="p-4 bg-black/30 border border-white/10">
                <p className="text-zinc-500 text-xs uppercase tracking-[0.12em]">User ID</p>
                <p className="text-zinc-100 mt-1 font-mono break-all">{account.user_id || 'Unknown'}</p>
              </div>
            </div>
          ) : null}

          {error ? <p className="text-red-400 text-sm mt-4">{error}</p> : null}
          {message ? <p className="text-emerald-300 text-sm mt-4">{message}</p> : null}
        </section>

        <section className="card-glass p-6">
          <h2 className="font-heading text-2xl font-bold">Security</h2>
          <p className="text-zinc-400 text-sm mt-2">
            Reset password is only available for Auth0 database users. Social logins reset via the identity provider.
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

        <section className="card-glass p-6 border border-red-500/25">
          <h2 className="font-heading text-2xl font-bold text-red-300">Delete Account</h2>
          <p className="text-zinc-400 text-sm mt-2">
            This deletes your tracked games and scan history from Sentient Tracker.
            {account?.management_delete_configured
              ? ' Your Auth0 identity is configured for automatic deletion.'
              : ' Auth0 identity auto-deletion is not configured, so dashboard cleanup may be required.'}
          </p>

          <div className="mt-4">
            <label className="text-xs text-zinc-500 uppercase tracking-[0.12em]">Type DELETE to confirm</label>
            <input
              type="text"
              value={confirmText}
              onChange={(e) => setConfirmText(e.target.value)}
              className="mt-2 w-full p-3 bg-black/40 border border-white/10 rounded text-white"
            />
          </div>

          <button
            type="button"
            onClick={handleDeleteAccount}
            disabled={deleting}
            className="mt-4 px-4 py-2 bg-red-500/20 border border-red-400/45 text-red-200 hover:text-white hover:border-red-300 disabled:opacity-60"
          >
            {deleting ? 'DELETING ACCOUNT...' : 'DELETE ACCOUNT'}
          </button>
        </section>
      </main>
    </div>
  );
};

export default Account;
