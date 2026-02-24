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
    <main className="page-shell max-w-4xl py-10 md:py-14">
      <section className="grid grid-cols-1 lg:grid-cols-[1.1fr_0.9fr] gap-4">
        <div className="card-glass p-6 md:p-8">
          <p className="section-eyebrow">Sentient Community</p>
          <h1 className="hero-title text-3xl md:text-5xl leading-tight mt-4">
            {isSignup ? 'Join players asking for better game updates.' : 'Log in and support the changes you want to see.'}
          </h1>
          <p className="hero-subtitle mt-4 text-base leading-relaxed max-w-xl">
            {isSignup
              ? 'Create petitions, support community requests, and help turn repeated player feedback into a stronger signal for studios.'
              : 'Pick up where you left off, manage your petitions, and support requests from other players.'}
          </p>

          <div className="mt-6">
            <PrimaryButton type="button" disabled={authLoading} onClick={action} className="w-full text-base py-4">
              {authLoading ? 'Authenticating…' : (isSignup ? 'Continue with Auth0 (Sign Up)' : 'Continue with Auth0 (Log In)')}
            </PrimaryButton>
          </div>

          {authError ? <p className="mt-4 text-sm text-rose-700 bg-rose-50 border border-rose-100 rounded-xl px-3 py-2">{authError}</p> : null}

          <div className="mt-6 text-sm text-slate-500">
            {isSignup ? (
              <>Already have an account? <Link to="/login" className="copy-link">Log in</Link></>
            ) : (
              <>New here? <Link to="/signup" className="copy-link">Create an account</Link></>
            )}
          </div>
        </div>

        <aside className="card-glass p-6 md:p-8">
          <h2 className="panel-title text-2xl font-bold">Why players use this</h2>
          <ul className="mt-4 space-y-3 text-sm text-slate-600 list-none p-0">
            <li className="feature-card">
              <h3 className="!text-base">One clear request</h3>
              <p>Turn scattered complaints into a focused ask that other players can support.</p>
            </li>
            <li className="feature-card">
              <h3 className="!text-base">Public progress</h3>
              <p>See supporter counts and milestones so momentum is visible, not vague.</p>
            </li>
            <li className="feature-card">
              <h3 className="!text-base">Better signal for studios</h3>
              <p>Milestone-backed petitions are easier to take seriously than isolated posts.</p>
            </li>
          </ul>
          <p className="text-xs text-slate-500 mt-4">
            Sign in uses secure Auth0 login. We do not ask you to manage a separate password inside the app.
          </p>
        </aside>
      </section>
    </main>
  );
}
