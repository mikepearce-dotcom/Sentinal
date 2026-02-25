import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth';
import { GhostButton } from './UI';

export default function AppFrame({ children }) {
  const { user, logout } = useAuth();
  const location = useLocation();
  const isPetitions = location.pathname.startsWith('/petitions');
  const isAccount = location.pathname.startsWith('/account');

  return (
    <div className="community-shell">
      <header className="community-header sticky top-0 z-30">
        <div className="max-w-7xl mx-auto px-4 md:px-8 py-4 flex items-center justify-between gap-4">
          <Link to="/" className="flex items-center gap-3 min-w-0">
            <span className="w-9 h-9 rounded-xl border border-sky-200 bg-white shadow-sm flex items-center justify-center">
              <span className="w-4 h-4 rounded-full bg-gradient-to-br from-sky-500 to-teal-400" />
            </span>
            <span className="min-w-0">
              <span className="block font-heading text-xl md:text-2xl font-bold tracking-tight text-slate-900">Sentient Community</span>
              <span className="hidden md:block text-xs text-slate-500">Player petitions for game improvements</span>
            </span>
          </Link>

          <nav className="flex items-center gap-2 md:gap-3 flex-wrap justify-end">
            <Link
              to="/petitions"
              className={`header-nav-link ${isPetitions ? 'header-nav-link-active' : ''}`}
            >
              Browse
            </Link>

            {user ? (
              <>
                <Link to="/petitions/new" className="header-nav-link">Create</Link>
                <Link to="/petitions/mine" className="header-nav-link">My Petitions</Link>
                <Link to="/account" className={`header-nav-link ${isAccount ? 'header-nav-link-active' : ''}`}>Account</Link>
                <div className="hidden md:flex header-user-pill max-w-[15rem]">
                  {user.avatar_url ? (
                    <img src={user.avatar_url} alt={user.name || 'User'} className="w-7 h-7 rounded-full object-cover" />
                  ) : (
                    <span className="w-7 h-7 rounded-full bg-sky-100 text-sky-700 flex items-center justify-center text-xs font-bold">
                      {String(user.name || user.email || 'U').slice(0, 1).toUpperCase()}
                    </span>
                  )}
                  <span className="text-xs text-slate-700 max-w-[10rem] truncate font-medium">{user.name || user.email}</span>
                </div>
                <GhostButton onClick={logout} className="text-sm">Logout</GhostButton>
              </>
            ) : (
              <>
                <Link to="/login" className="header-nav-link">Log In</Link>
                <Link to="/signup" className="btn-primary px-4 py-2 text-sm">
                  <span>Start Petition</span>
                </Link>
              </>
            )}
          </nav>
        </div>
      </header>
      {children}
    </div>
  );
}
