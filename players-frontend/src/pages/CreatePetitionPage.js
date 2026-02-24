import React, { useEffect, useState } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import api from '../api/axios';
import { PrimaryButton, Tag } from '../components/UI';
import { formatNumber, prettyLabel, toArray } from '../lib/community';
import { useAuth } from '../hooks/useAuth';

export default function CreatePetitionPage() {
  const { user } = useAuth();
  const navigate = useNavigate();
  const [metadata, setMetadata] = useState({ categories: [], change_types: [] });
  const [gameQuery, setGameQuery] = useState('');
  const [gameSuggestions, setGameSuggestions] = useState([]);
  const [selectedGame, setSelectedGame] = useState(null);
  const [form, setForm] = useState({
    title: '',
    summary: '',
    body: '',
    category: 'gameplay',
    change_type: 'feature_request',
  });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!user) return;
    api.get('/api/community/metadata')
      .then((resp) => {
        const data = resp?.data || {};
        setMetadata(data);
        setForm((prev) => ({
          ...prev,
          category: (toArray(data.categories)[0] || prev.category),
          change_type: (toArray(data.change_types)[0] || prev.change_type),
        }));
      })
      .catch(() => {});
  }, [user]);

  useEffect(() => {
    let cancelled = false;
    const q = String(gameQuery || '').trim();
    if (q.length < 2 || selectedGame) {
      setGameSuggestions([]);
      return () => { cancelled = true; };
    }

    const timer = setTimeout(() => {
      api.get('/api/community/games/search', { params: { q, limit: 8 } })
        .then((resp) => {
          if (!cancelled) setGameSuggestions(toArray(resp?.data));
        })
        .catch(() => {
          if (!cancelled) setGameSuggestions([]);
        });
    }, 250);

    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [gameQuery, selectedGame]);

  if (!user) return <Navigate to="/login" replace />;

  const onSubmit = async (e) => {
    e.preventDefault();
    setSaving(true);
    setError('');
    try {
      const payload = {
        game_id: selectedGame?.id || '',
        game_name: selectedGame?.name || gameQuery,
        title: form.title,
        summary: form.summary,
        body: form.body,
        category: form.category,
        change_type: form.change_type,
      };
      const resp = await api.post('/api/community/petitions', payload);
      const slug = resp?.data?.slug || resp?.data?.id;
      navigate(slug ? `/petitions/${slug}` : '/petitions');
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : 'Failed to create petition.');
    } finally {
      setSaving(false);
    }
  };

  return (
    <main className="max-w-5xl mx-auto px-4 md:px-8 py-8">
      <section className="card-glass p-6 md:p-8">
        <h1 className="font-heading text-4xl font-black">Create a Petition</h1>
        <p className="mt-2 text-zinc-400 text-sm">
          Strong petitions are specific. Define the change, the affected players, and what outcome the studio should prioritize.
        </p>

        <form onSubmit={onSubmit} className="mt-6 space-y-5">
          <div>
            <label className="block text-sm text-zinc-300 mb-2">Game</label>
            <input
              value={selectedGame?.name || gameQuery}
              onChange={(e) => {
                setSelectedGame(null);
                setGameQuery(e.target.value);
              }}
              placeholder="Search or type a game name"
              className="w-full bg-black/30 border border-white/10 px-3 py-3 text-zinc-100 placeholder:text-zinc-500"
            />

            {selectedGame ? (
              <div className="mt-2 flex items-center gap-2 text-xs text-zinc-400 flex-wrap">
                <Tag tone="success">Selected</Tag>
                <span>{selectedGame.name}</span>
                <button type="button" className="text-zinc-500 hover:text-zinc-300" onClick={() => setSelectedGame(null)}>Clear</button>
              </div>
            ) : null}

            {!selectedGame && gameSuggestions.length > 0 ? (
              <div className="mt-2 border border-white/10 bg-black/30 max-h-56 overflow-y-auto">
                {gameSuggestions.map((game) => (
                  <button
                    type="button"
                    key={`${game.id || game.slug}-${game.name}`}
                    onClick={() => {
                      setSelectedGame(game);
                      setGameQuery(game.name || '');
                      setGameSuggestions([]);
                    }}
                    className="w-full text-left px-3 py-2 border-b border-white/5 last:border-b-0 hover:bg-white/[0.03]"
                  >
                    <div className="flex items-center justify-between gap-3">
                      <span className="text-sm text-zinc-100">{game.name}</span>
                      <span className="text-xs text-zinc-500">
                        {game.source === 'catalog' ? `${formatNumber(game.petition_count)} petitions` : 'Suggested'}
                      </span>
                    </div>
                  </button>
                ))}
              </div>
            ) : null}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm text-zinc-300 mb-2">Category</label>
              <select value={form.category} onChange={(e) => setForm((p) => ({ ...p, category: e.target.value }))} className="w-full bg-black/30 border border-white/10 px-3 py-3 text-zinc-100">
                {toArray(metadata.categories).map((item) => <option key={item} value={item}>{prettyLabel(item)}</option>)}
              </select>
            </div>
            <div>
              <label className="block text-sm text-zinc-300 mb-2">Type of Change</label>
              <select value={form.change_type} onChange={(e) => setForm((p) => ({ ...p, change_type: e.target.value }))} className="w-full bg-black/30 border border-white/10 px-3 py-3 text-zinc-100">
                {toArray(metadata.change_types).map((item) => <option key={item} value={item}>{prettyLabel(item)}</option>)}
              </select>
            </div>
          </div>

          <div>
            <label className="block text-sm text-zinc-300 mb-2">Petition Title</label>
            <input value={form.title} onChange={(e) => setForm((p) => ({ ...p, title: e.target.value }))} maxLength={160} placeholder="Example: Add a PvE-only queue for solo players" className="w-full bg-black/30 border border-white/10 px-3 py-3 text-zinc-100 placeholder:text-zinc-500" />
          </div>

          <div>
            <label className="block text-sm text-zinc-300 mb-2">Short Summary</label>
            <textarea value={form.summary} onChange={(e) => setForm((p) => ({ ...p, summary: e.target.value }))} rows={3} maxLength={300} placeholder="One paragraph on the request and why players should support it." className="w-full bg-black/30 border border-white/10 px-3 py-3 text-zinc-100 placeholder:text-zinc-500" />
          </div>

          <div>
            <label className="block text-sm text-zinc-300 mb-2">Detailed Request</label>
            <textarea value={form.body} onChange={(e) => setForm((p) => ({ ...p, body: e.target.value }))} rows={10} maxLength={8000} placeholder="Describe the current issue, who it affects, and what the studio should change." className="w-full bg-black/30 border border-white/10 px-3 py-3 text-zinc-100 placeholder:text-zinc-500" />
          </div>

          {error ? <p className="text-sm text-amber-300">{error}</p> : null}

          <div className="flex items-center justify-between gap-3 flex-wrap">
            <p className="text-xs text-zinc-500">Petitions are public and shareable. Keep requests specific and respectful.</p>
            <PrimaryButton type="submit" disabled={saving}>{saving ? 'Creating…' : 'Publish Petition'}</PrimaryButton>
          </div>
        </form>
      </section>
    </main>
  );
}
