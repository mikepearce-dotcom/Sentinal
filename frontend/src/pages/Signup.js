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

  return err?.message || 'Failed to create account. Please try again.';
};

const Signup = () => {
  const { signup, authLoading, authError } = useContext(AuthContext);
  const [error, setError] = useState('');

  const handleSignup = async () => {
    setError('');

    try {
      await signup();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  };

  return (
    <div className="min-h-screen bg-[#09090b] hero-glow px-6 py-12 flex items-center justify-center">
      <div className="w-full max-w-md card-glass p-8">
        <p className="font-mono text-xs text-[#D3F34B] tracking-widest uppercase">Sentient Tracker</p>
        <h1 className="font-heading text-3xl font-black mt-3">Create Account</h1>
        <p className="text-zinc-400 mt-2 text-sm">Create your account with secure Auth0 signup.</p>

        <button
          type="button"
          onClick={handleSignup}
          disabled={authLoading}
          className="btn-primary w-full py-3 mt-6 disabled:opacity-60"
        >
          <span>{authLoading ? 'REDIRECTING...' : 'SIGN UP WITH AUTH0'}</span>
        </button>

        {error ? <p className="text-red-400 text-sm mt-4">{error}</p> : null}
        {authError ? <p className="text-amber-300 text-sm mt-2">{authError}</p> : null}

        <p className="text-sm text-zinc-400 mt-6">
          Already have an account?{' '}
          <Link to="/login" className="text-[#D3F34B] hover:underline">
            Log in
          </Link>
        </p>
      </div>
    </div>
  );
};

export default Signup;
