import React, { useEffect, useState } from 'react';
import { Navigate, Link, useSearchParams } from 'react-router-dom';
import api from '../api/axios';
import PetitionCard from '../components/PetitionCard';
import { GhostButton } from '../components/UI';
import { toArray } from '../lib/community';
import { useAuth } from '../hooks/useAuth';

export default function PetitionsPage({ mineOnly = false }) {
  const { user } = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [metadata, setMetadata] = useState({ categories: [] });

  const page = Math.max(1, Number(searchParams.get('page') || 1) || 1);
  const q = searchParams.get('q') || '';
  const category = searchParams.get('category') || '';
  const sort = searchParams.get('sort') || 'momentum';
  const pageSize = 18;

  useEffect(() => {
    let cancelled = false;
    api.get('/api/community/metadata')
      .then((resp) => {
        if (!cancelled) setMetadata(resp?.data || { categories: [] });
      })
      .catch(() => {});
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError('');

    const req = mineOnly && user
      ? api.get('/api/community/petitions/mine')
      : api.get('/api/community/petitions', { params: { page, limit: pageSize, q, category, sort } });

    req.then((resp) => {
      if (cancelled) return;
      if (mineOnly) {
        const nextItems = toArray(resp?.data?.items);
        setItems(nextItems);
        setTotal(nextItems.length);
      } else {
        setItems(toArray(resp?.data?.items));
        setTotal(Number(resp?.data?.total || 0));
      }
    })
      .catch((err) => {
        if (cancelled) return;
        const detail = err?.response?.data?.detail;
        setError(typeof detail === 'string' ? detail : 'Failed to load petitions.');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [mineOnly, user, page, q, category, sort]);

  const totalPages = mineOnly ? 1 : Math.max(1, Math.ceil(total / pageSize));

  const updateParam = (key, value) => {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(key, value);
    else next.delete(key);
    if (key !== 'page') next.set('page', '1');
    setSearchParams(next);
  };

  if (mineOnly && !user) return <Navigate to="/login" replace />;

  return (
    <main className="max-w-7xl mx-auto px-4 md:px-8 py-8">
      <section className="card-glass p-6">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div>
            <h1 className="font-heading text-4xl font-black">{mineOnly ? 'My Petitions' : 'Community Petitions'}</h1>
            <p className="mt-2 text-zinc-400 text-sm">
              {mineOnly
                ? 'Track the petitions you have created and share them with other players.'
                : 'Browse, support, and share player-driven change requests by game and topic.'}
            </p>
          </div>
          <Link to="/petitions/new" className="btn-primary px-4 py-2 text-sm"><span>Create Petition</span></Link>
        </div>

        {!mineOnly ? (
          <div className="mt-5 grid grid-cols-1 md:grid-cols-[1.4fr_0.8fr_0.8fr] gap-3">
            <input
              value={q}
              onChange={(e) => updateParam('q', e.target.value)}
              placeholder="Search petitions, games, or topics"
              className="w-full bg-black/30 border border-white/10 px-3 py-3 text-zinc-100 placeholder:text-zinc-500"
            />
            <select value={category} onChange={(e) => updateParam('category', e.target.value)} className="bg-black/30 border border-white/10 px-3 py-3 text-zinc-100">
              <option value="">All categories</option>
              {toArray(metadata.categories).map((item) => (
                <option key={item} value={item}>{String(item).replace(/_/g, ' ').replace(/\b\w/g, (m) => m.toUpperCase())}</option>
              ))}
            </select>
            <select value={sort} onChange={(e) => updateParam('sort', e.target.value)} className="bg-black/30 border border-white/10 px-3 py-3 text-zinc-100">
              <option value="momentum">Momentum</option>
              <option value="top">Top supporters</option>
              <option value="new">Newest</option>
            </select>
          </div>
        ) : null}
      </section>

      <section className="mt-6">
        {loading ? (
          <div className="card-glass p-6 text-zinc-400">Loading petitions...</div>
        ) : error ? (
          <div className="card-glass p-6 text-amber-300">{error}</div>
        ) : items.length === 0 ? (
          <div className="card-glass p-6 text-zinc-400">No petitions found yet.</div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
            {items.map((item) => <PetitionCard key={item.id || item.slug} petition={item} />)}
          </div>
        )}
      </section>

      {!mineOnly && totalPages > 1 ? (
        <div className="mt-6 flex items-center justify-between gap-3">
          <GhostButton disabled={page <= 1} onClick={() => updateParam('page', String(page - 1))}>Previous</GhostButton>
          <span className="text-sm text-zinc-400">Page {page} / {totalPages}</span>
          <GhostButton disabled={page >= totalPages} onClick={() => updateParam('page', String(page + 1))}>Next</GhostButton>
        </div>
      ) : null}
    </main>
  );
}
