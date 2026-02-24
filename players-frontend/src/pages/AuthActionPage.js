import React, { useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { PrimaryButton } from '../components/UI';
import { useAuth } from '../hooks/useAuth';

export default function AuthActionPage({ mode = 'login' }) {
  const { login, signup, authError, authLoading, user } = useAuth();
  const navigate = useNavigate();
  const isSignup = mode === 'signup';

  useEffect(() => {
    if (user) navigate('/petitions');
  }, [navigate, user]);

  const action = isSignup ? signup : login;

  return (
    <main className="max-w-xl mx-auto px-4 md:px-8 py-14">
      <section className="card-glass p-6 md:p-8">
        <p className="font-mono text-xs uppercase tracking-[0.2em] text-[#8BE8FF]">Sentient Community</p>
        <h1 className="font-heading text-4xl font-black mt-3">
          {isSignup ? 'Join and start petitions' : 'Log in to support petitions'}
        </h1>
        <p className="mt-3 text-zinc-300">
          {isSignup
            ? 'Create petitions, support community requests, and help push milestone-backed changes to studios.'
            : 'Support petitions, manage your submissions, and share player-led requests.'}
        </p>

        <div className="mt-6">
          <PrimaryButton type="button" disabled={authLoading} onClick={action} className="w-full text-base py-4">
            {authLoading ? 'Authenticating…' : (isSignup ? 'Continue with Auth0 (Sign Up)' : 'Continue with Auth0 (Log In)')}
          </PrimaryButton>
        </div>

        {authError ? <p className="mt-4 text-sm text-amber-300">{authError}</p> : null}

        <div className="mt-6 text-sm text-zinc-400">
          {isSignup ? (
            <>Already have an account? <Link to="/login" className="text-zinc-200 hover:text-white">Log in</Link></>
          ) : (
            <>New here? <Link to="/signup" className="text-zinc-200 hover:text-white">Create an account</Link></>
          )}
        </div>
      </section>
    </main>
  );
}
