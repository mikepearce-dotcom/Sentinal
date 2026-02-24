import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import api from '../api/axios';
import PetitionCard from '../components/PetitionCard';
import { toArray } from '../lib/community';
import { useAuth } from '../hooks/useAuth';

export default function LandingPage() {
  const { user } = useAuth();
  const [featured, setFeatured] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;

    api.get('/api/community/petitions', { params: { sort: 'momentum', limit: 6, page: 1 } })
      .then((resp) => {
        if (!cancelled) setFeatured(toArray(resp?.data?.items));
      })
      .catch((err) => {
        if (!cancelled) {
          const detail = err?.response?.data?.detail;
          setError(typeof detail === 'string' ? detail : 'Failed to load featured petitions.');
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <main>
      <section className="hero-glow border-b border-white/5">
        <div className="max-w-7xl mx-auto px-4 md:px-8 py-14 md:py-20 grid grid-cols-1 xl:grid-cols-[1.2fr_0.8fr] gap-8 items-start">
          <div>
            <p className="font-mono text-xs uppercase tracking-[0.22em] text-[#8BE8FF]">Player-led game change requests</p>
            <h1 className="font-heading text-5xl md:text-7xl font-black leading-[0.95] mt-4">
              Organize player demand and push it to studios with evidence.
            </h1>
            <p className="mt-5 text-lg text-zinc-300 max-w-2xl leading-relaxed">
              Create petitions for specific game changes, collect supporters, and build milestone-backed signals studios can take seriously.
              This is community advocacy for games, built for repeatable product feedback.
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <Link to="/petitions" className="btn-primary px-5 py-3 text-sm"><span>Explore Petitions</span></Link>
              {user ? (
                <Link to="/petitions/new" className="px-5 py-3 border border-white/15 text-zinc-200 hover:text-white hover:border-white/30 text-sm">
                  Start a Petition
                </Link>
              ) : (
                <Link to="/signup" className="px-5 py-3 border border-white/15 text-zinc-200 hover:text-white hover:border-white/30 text-sm">
                  Sign Up to Support
                </Link>
              )}
            </div>
          </div>

          <div className="card-glass p-6">
            <h2 className="font-heading text-2xl font-bold">How it works</h2>
            <ol className="mt-4 space-y-4 text-sm text-zinc-300">
              <li><span className="font-mono text-[#D3F34B] mr-2">01</span>Choose the game and define the change clearly (gameplay, content, balance, performance, etc).</li>
              <li><span className="font-mono text-[#D3F34B] mr-2">02</span>Collect supporters and share the petition across Reddit, Discord, and community spaces.</li>
              <li><span className="font-mono text-[#D3F34B] mr-2">03</span>Hit milestones and create a stronger signal that studios can review.</li>
            </ol>
            <div className="mt-5 grid grid-cols-2 gap-3 text-xs">
              <div className="border border-white/10 p-3 bg-black/20">
                <p className="text-zinc-500">Milestones</p>
                <p className="mt-1 text-zinc-100">100 / 500 / 1000+</p>
              </div>
              <div className="border border-white/10 p-3 bg-black/20">
                <p className="text-zinc-500">Public petition pages</p>
                <p className="mt-1 text-zinc-100">Built in</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="max-w-7xl mx-auto px-4 md:px-8 py-10">
        <div className="flex items-center justify-between gap-3 mb-5">
          <h2 className="font-heading text-4xl font-black">Petitions Gaining Momentum</h2>
          <Link to="/petitions" className="text-sm text-zinc-300 hover:text-white">View all</Link>
        </div>
        {loading ? (
          <div className="card-glass p-6 text-zinc-400">Loading petitions...</div>
        ) : error ? (
          <div className="card-glass p-6 text-amber-300">{error}</div>
        ) : featured.length === 0 ? (
          <div className="card-glass p-6 text-zinc-400">No petitions yet. Be the first to start one.</div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
            {featured.map((item) => <PetitionCard key={item.id || item.slug} petition={item} />)}
          </div>
        )}
      </section>
    </main>
  );
}
