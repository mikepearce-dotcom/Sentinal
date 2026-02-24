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
      <section className="page-shell pt-8 md:pt-12">
        <div className="hero-grid">
          <div className="hero-panel p-6 md:p-8 xl:p-10">
            <p className="section-eyebrow">Player petitions that move studios</p>
            <h1 className="hero-title text-4xl md:text-6xl leading-[0.95] mt-4 max-w-3xl">
              Turn community frustration into a clear, shareable request.
            </h1>
            <p className="hero-subtitle mt-5 text-base md:text-lg leading-relaxed max-w-2xl">
              Start a petition for a real game change, collect supporters, and track progress toward milestones.
              When enough players back the same request, it becomes much harder to ignore.
            </p>

            <div className="mt-7 flex flex-wrap gap-3">
              <Link to="/petitions" className="btn-primary px-5 py-3 text-sm md:text-base">
                <span>Browse Petitions</span>
              </Link>
              {user ? (
                <Link to="/petitions/new" className="btn-secondary px-5 py-3 text-sm md:text-base font-semibold">
                  Start a Petition
                </Link>
              ) : (
                <Link to="/signup" className="btn-secondary px-5 py-3 text-sm md:text-base font-semibold">
                  Create Account to Start
                </Link>
              )}
            </div>

            <div className="mt-8 grid grid-cols-1 sm:grid-cols-3 gap-3">
              <div className="feature-card">
                <h3>Clear asks</h3>
                <p>Keep requests focused on one change so more players understand and support it.</p>
              </div>
              <div className="feature-card">
                <h3>Public momentum</h3>
                <p>Track supporters and milestone progress with a link you can share anywhere.</p>
              </div>
              <div className="feature-card">
                <h3>Studio-ready signal</h3>
                <p>Milestone-backed petitions can be surfaced as stronger evidence of player demand.</p>
              </div>
            </div>
          </div>

          <div className="hero-panel p-4 md:p-5">
            <div className="hero-visual">
              <div className="hero-core" />
              <div className="hero-ring hero-ring-a" />
              <div className="hero-ring hero-ring-b" />
              <div className="hero-ring hero-ring-c" />
              <div className="hero-beam" />

              <div className="hero-card hero-card-a">
                <p className="hero-card-title">Petition growth</p>
                <p className="hero-card-body">Players are backing one clear request instead of scattering across dozens of posts.</p>
                <p className="hero-card-metric">+142 supporters this week</p>
              </div>

              <div className="hero-card hero-card-b">
                <p className="hero-card-title">Milestone tracking</p>
                <p className="hero-card-body">Visible progress gives communities a reason to share and keep pushing.</p>
                <p className="hero-card-metric">Next milestone: 500 supporters</p>
              </div>

              <div className="hero-card hero-card-c">
                <p className="hero-card-title">Studio handoff</p>
                <p className="hero-card-body">High-support requests can be surfaced as stronger player demand signals.</p>
                <p className="hero-card-metric">Evidence-backed, easier to review</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="page-shell pt-0">
        <div className="card-glass p-6 md:p-8">
          <div className="flex items-start justify-between gap-4 flex-wrap">
            <div>
              <p className="section-eyebrow">How it works</p>
              <h2 className="panel-title text-3xl md:text-4xl font-bold mt-3">Simple steps. Real community momentum.</h2>
              <p className="muted-copy mt-3 max-w-2xl">
                You do not need a perfect essay. Start with one clear change, explain why it matters, and share it with players who care.
              </p>
            </div>
            <Link to={user ? '/petitions/new' : '/signup'} className="btn-secondary px-4 py-2 text-sm font-semibold">
              {user ? 'Create a Petition' : 'Join Free'}
            </Link>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6">
            <div className="feature-card">
              <h3>1. Start a clear request</h3>
              <p>Pick a game, choose the type of change, and explain exactly what should improve.</p>
            </div>
            <div className="feature-card">
              <h3>2. Share and collect support</h3>
              <p>Post the petition link in Reddit, Discord, and community spaces where players already talk.</p>
            </div>
            <div className="feature-card">
              <h3>3. Reach milestones</h3>
              <p>As support grows, the request becomes a stronger signal that can be surfaced to studios.</p>
            </div>
          </div>
        </div>
      </section>

      <section className="page-shell pt-0 pb-8">
        <div className="flex items-center justify-between gap-3 mb-5 flex-wrap">
          <div>
            <p className="section-eyebrow">Trending now</p>
            <h2 className="panel-title text-3xl md:text-4xl font-bold mt-2">Petitions gaining support</h2>
          </div>
          <Link to="/petitions" className="copy-link text-sm">View all petitions</Link>
        </div>

        {loading ? (
          <div className="card-glass p-6 text-slate-500">Loading petitions...</div>
        ) : error ? (
          <div className="card-glass p-6 text-amber-700">{error}</div>
        ) : featured.length === 0 ? (
          <div className="card-glass p-6 text-slate-500">No petitions yet. Be the first player to start one.</div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
            {featured.map((item) => <PetitionCard key={item.id || item.slug} petition={item} />)}
          </div>
        )}
      </section>
    </main>
  );
}
