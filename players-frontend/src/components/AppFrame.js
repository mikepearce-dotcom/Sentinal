import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth';
import { GhostButton } from './UI';

export default function AppFrame({ children }) {
  const { user, logout } = useAuth();
  const location = useLocation();
  const isPetitions = location.pathname.startsWith('/petitions');

  return (
    <div className="min-h-screen bg-[#09090b] text-white">
      <header className="border-b border-white/5 sticky top-0 z-30 backdrop-blur bg-[#09090b]/85">
        <div className="max-w-7xl mx-auto px-4 md:px-8 py-4 flex items-center justify-between gap-4">
          <Link to="/" className="font-heading text-2xl font-black tracking-tight flex items-center gap-2">
            <span className="text-[#00E5FF]">Sentient</span>
            <span className="text-[#D3F34B]">Community</span>
          </Link>

          <nav className="flex items-center gap-2 md:gap-3 flex-wrap justify-end">
            <Link
              to="/petitions"
              className={`px-3 py-2 text-sm border ${
                isPetitions
                  ? 'border-[#D3F34B]/40 text-[#e7ff8b] bg-[#D3F34B]/10'
                  : 'border-white/15 text-zinc-300 hover:text-white hover:border-white/30'
              }`}
            >
              Browse Petitions
            </Link>

            {user ? (
              <>
                <Link to="/petitions/new" className="px-3 py-2 text-sm border border-white/15 text-zinc-300 hover:text-white hover:border-white/30">
                  Create Petition
                </Link>
                <Link to="/petitions/mine" className="px-3 py-2 text-sm border border-white/15 text-zinc-300 hover:text-white hover:border-white/30">
                  My Petitions
                </Link>
                <div className="hidden md:flex items-center gap-2 px-3 py-2 border border-white/10 bg-black/30">
                  {user.avatar_url ? (
                    <img src={user.avatar_url} alt={user.name || 'User'} className="w-6 h-6 rounded-full object-cover" />
                  ) : null}
                  <span className="text-xs text-zinc-300 max-w-[11rem] truncate">{user.name || user.email}</span>
                </div>
                <GhostButton onClick={logout} className="text-sm">Logout</GhostButton>
              </>
            ) : (
              <>
                <Link to="/login" className="px-3 py-2 text-sm border border-white/15 text-zinc-300 hover:text-white hover:border-white/30">
                  Log In
                </Link>
                <Link to="/signup" className="btn-primary px-4 py-2 text-sm">
                  <span>Join & Sign</span>
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
