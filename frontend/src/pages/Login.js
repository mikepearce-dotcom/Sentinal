import React, { useContext, useState } from 'react';
import { Link } from 'react-router-dom';
import { AuthContext } from '../context/AuthContext';

const getErrorMessage = (err) => {
  const detail = err?.response?.data?.detail;

  if (Array.isArray(detail)) {
    return detail.map((item) => item?.msg || 'Invalid input').join(', ');
  }

  if (typeof detail === 'string' && detail.trim()) {
    return detail;
  }

  if (!err?.response) {
    return 'Unable to reach backend. Check REACT_APP_BACKEND_URL and backend health.';
  }

  return 'Login failed. Please try again.';
};

const Login = () => {
  const { login } = useContext(AuthContext);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setSubmitting(true);

    try {
      await login(email, password);
    } catch (err) {
      setError(getErrorMessage(err));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#09090b] hero-glow px-6 py-12 flex items-center justify-center">
      <div className="w-full max-w-md card-glass p-8">
        <p className="font-mono text-xs text-[#D3F34B] tracking-widest uppercase">Sentient Tracker</p>
        <h1 className="font-heading text-3xl font-black mt-3">Log In</h1>
        <p className="text-zinc-400 mt-2 text-sm">Sign in to manage tracked games and run scans.</p>

        <form onSubmit={handleSubmit} className="mt-6 space-y-3">
          <input
            type="email"
            placeholder="Email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className="w-full p-3 bg-black/40 border border-white/10 rounded"
            required
          />
          <input
            type="password"
            placeholder="Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="w-full p-3 bg-black/40 border border-white/10 rounded"
            required
          />
          <button type="submit" disabled={submitting} className="btn-primary w-full py-3 disabled:opacity-60">
            <span>{submitting ? 'LOGGING IN...' : 'LOG IN'}</span>
          </button>
        </form>

        {error ? <p className="text-red-400 text-sm mt-4">{error}</p> : null}

        <p className="text-sm text-zinc-400 mt-6">
          Need an account?{' '}
          <Link to="/signup" className="text-[#D3F34B] hover:underline">
            Sign up
          </Link>
        </p>
      </div>
    </div>
  );
};

export default Login;
