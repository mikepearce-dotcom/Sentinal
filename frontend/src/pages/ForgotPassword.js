import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import api from '../api/axios';

const getErrorMessage = (err) => {
  const detail = err?.response?.data?.detail;

  if (Array.isArray(detail)) {
    return detail.map((item) => item?.msg || 'Invalid input').join(', ');
  }

  if (typeof detail === 'string' && detail.trim()) {
    return detail;
  }

  return err?.message || 'Could not send password reset email.';
};

const ForgotPassword = () => {
  const [email, setEmail] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setMessage('');

    const normalized = email.trim().toLowerCase();
    if (!normalized) {
      setError('Enter the email for your account.');
      return;
    }

    setLoading(true);
    try {
      const resp = await api.post('/api/auth/password-reset-request', { email: normalized });
      setMessage(resp?.data?.message || 'If an account exists, a password reset email has been sent.');
    } catch (err) {
      setError(getErrorMessage(err));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#09090b] hero-glow px-6 py-12 flex items-center justify-center">
      <div className="w-full max-w-md card-glass p-8">
        <p className="font-mono text-xs text-[#D3F34B] tracking-widest uppercase">Sentient Tracker</p>
        <h1 className="font-heading text-3xl font-black mt-3">Reset Password</h1>
        <p className="text-zinc-400 mt-2 text-sm">
          We&apos;ll email you a reset link if this account uses email/password login.
        </p>

        <form onSubmit={handleSubmit} className="mt-6 space-y-3">
          <input
            type="email"
            placeholder="you@studio.com"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className="w-full p-3 bg-black/40 border border-white/10 rounded text-white"
          />

          <button type="submit" disabled={loading} className="btn-primary w-full py-3 disabled:opacity-60">
            <span>{loading ? 'SENDING...' : 'SEND RESET EMAIL'}</span>
          </button>
        </form>

        {error ? <p className="text-red-400 text-sm mt-4">{error}</p> : null}
        {message ? <p className="text-emerald-300 text-sm mt-4">{message}</p> : null}

        <p className="text-sm text-zinc-400 mt-6">
          <Link to="/login" className="text-[#D3F34B] hover:underline">
            Back to login
          </Link>
        </p>
      </div>
    </div>
  );
};

export default ForgotPassword;
