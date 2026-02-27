import React, { useEffect, useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth';
import { GhostButton } from './UI';

export default function AppFrame({ children }) {
  const { user, logout } = useAuth();
  const location = useLocation();
  const [menuOpen, setMenuOpen] = useState(false);
  const isPetitions = location.pathname.startsWith('/petitions');
  const isAccount = location.pathname.startsWith('/account');
  const displayName = user?.name || user?.email || 'User';
  const userInitial = String(displayName).slice(0, 1).toUpperCase();

  useEffect(() => {
    setMenuOpen(false);
  }, [location.pathname]);

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

          <nav className="hidden md:flex items-center gap-2 md:gap-3 flex-wrap justify-end">
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
                    <img src={user.avatar_url} alt={displayName} className="w-7 h-7 rounded-full object-cover" />
                  ) : (
                    <span className="w-7 h-7 rounded-full bg-sky-100 text-sky-700 flex items-center justify-center text-xs font-bold">
                      {userInitial}
                    </span>
                  )}
                  <span className="text-xs text-slate-700 max-w-[10rem] truncate font-medium">{displayName}</span>
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

          <button
            type="button"
            className={`mobile-menu-toggle md:hidden ${menuOpen ? 'mobile-menu-toggle-open' : ''}`.trim()}
            aria-label={menuOpen ? 'Close menu' : 'Open menu'}
            aria-expanded={menuOpen}
            onClick={() => setMenuOpen((value) => !value)}
          >
            <span className="mobile-menu-toggle-bar" />
            <span className="mobile-menu-toggle-bar" />
            <span className="mobile-menu-toggle-bar" />
          </button>
        </div>

        {menuOpen ? (
          <div className="md:hidden px-4 pb-4">
            <div className="mobile-nav-panel">
              {user ? (
                <div className="mobile-user-summary">
                  {user.avatar_url ? (
                    <img src={user.avatar_url} alt={displayName} className="w-9 h-9 rounded-full object-cover" />
                  ) : (
                    <span className="w-9 h-9 rounded-full bg-sky-100 text-sky-700 flex items-center justify-center text-sm font-bold">
                      {userInitial}
                    </span>
                  )}
                  <div className="min-w-0">
                    <p className="text-sm font-semibold text-slate-900 truncate">{displayName}</p>
                    <p className="text-xs text-slate-500 truncate">{user.email}</p>
                  </div>
                </div>
              ) : null}

              <nav className="mobile-nav-links">
                <Link
                  to="/petitions"
                  className={`header-nav-link mobile-nav-link ${isPetitions ? 'header-nav-link-active' : ''}`}
                >
                  Browse petitions
                </Link>

                {user ? (
                  <>
                    <Link to="/petitions/new" className="header-nav-link mobile-nav-link">Create a petition</Link>
                    <Link to="/petitions/mine" className="header-nav-link mobile-nav-link">My petitions</Link>
                    <Link to="/account" className={`header-nav-link mobile-nav-link ${isAccount ? 'header-nav-link-active' : ''}`}>Account</Link>
                    <GhostButton onClick={logout} className="w-full justify-center text-sm">Logout</GhostButton>
                  </>
                ) : (
                  <>
                    <Link to="/login" className="header-nav-link mobile-nav-link">Log In</Link>
                    <Link to="/signup" className="btn-primary mobile-nav-cta px-4 py-3 text-sm">
                      <span>Start Petition</span>
                    </Link>
                  </>
                )}
              </nav>
            </div>
          </div>
        ) : null}
      </header>
      {children}
    </div>
  );
}
