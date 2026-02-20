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

  return err?.message || 'Login failed. Please try again.';
};

const Login = () => {
  const { login, authLoading, authError } = useContext(AuthContext);
  const [error, setError] = useState('');

  const handleLogin = async () => {
    setError('');

    try {
      await login();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  };

  return (
    <div className="min-h-screen bg-[#09090b] hero-glow px-6 py-12 flex items-center justify-center">
      <div className="w-full max-w-md card-glass p-8">
        <p className="font-mono text-xs text-[#D3F34B] tracking-widest uppercase">Sentient Tracker</p>
        <h1 className="font-heading text-3xl font-black mt-3">Log In</h1>
        <p className="text-zinc-400 mt-2 text-sm">Use secure Auth0 login to access your sentiment dashboard.</p>

        <button
          type="button"
          onClick={handleLogin}
          disabled={authLoading}
          className="btn-primary w-full py-3 mt-6 disabled:opacity-60"
        >
          <span>{authLoading ? 'REDIRECTING...' : 'CONTINUE WITH AUTH0'}</span>
        </button>

        {error ? <p className="text-red-400 text-sm mt-4">{error}</p> : null}
        {authError ? <p className="text-amber-300 text-sm mt-2">{authError}</p> : null}

        <div className="mt-6 flex items-center justify-between gap-4 text-sm text-zinc-400">
          <Link to="/forgot-password" className="text-[#8BE8FF] hover:underline">
            Forgot password?
          </Link>

          <p>
            Need an account?{' '}
            <Link to="/signup" className="text-[#D3F34B] hover:underline">
              Sign up
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
};

export default Login;
