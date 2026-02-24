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
    <main className="page-shell max-w-5xl">
      <section className="card-glass p-6 md:p-8">
        <p className="section-eyebrow">Create petition</p>
        <h1 className="panel-title text-3xl md:text-4xl font-bold mt-2">Start a clear request players can support</h1>
        <p className="mt-2 text-slate-500 text-sm max-w-3xl">
          Keep it specific. Focus on one change, explain who it helps, and describe what a better outcome looks like.
        </p>

        <form onSubmit={onSubmit} className="mt-6 space-y-5">
          <div>
            <label className="field-label">Game</label>
            <input
              value={selectedGame?.name || gameQuery}
              onChange={(e) => {
                setSelectedGame(null);
                setGameQuery(e.target.value);
              }}
              placeholder="Search or type a game name"
              className="field-input"
            />

            {selectedGame ? (
              <div className="mt-2 flex items-center gap-2 text-xs text-slate-500 flex-wrap">
                <Tag tone="success">Selected</Tag>
                <span>{selectedGame.name}</span>
                <button type="button" className="copy-link text-xs" onClick={() => setSelectedGame(null)}>Clear</button>
              </div>
            ) : null}

            {!selectedGame && gameSuggestions.length > 0 ? (
              <div className="mt-2 border border-slate-200 bg-white rounded-xl max-h-56 overflow-y-auto shadow-sm">
                {gameSuggestions.map((game) => (
                  <button
                    type="button"
                    key={`${game.id || game.slug}-${game.name}`}
                    onClick={() => {
                      setSelectedGame(game);
                      setGameQuery(game.name || '');
                      setGameSuggestions([]);
                    }}
                    className="w-full text-left px-3 py-2 border-b border-slate-100 last:border-b-0 hover:bg-slate-50"
                  >
                    <div className="flex items-center justify-between gap-3">
                      <span className="text-sm text-slate-900">{game.name}</span>
                      <span className="text-xs text-slate-500">
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
              <label className="field-label">Category</label>
              <select value={form.category} onChange={(e) => setForm((p) => ({ ...p, category: e.target.value }))} className="field-select">
                {toArray(metadata.categories).map((item) => <option key={item} value={item}>{prettyLabel(item)}</option>)}
              </select>
            </div>
            <div>
              <label className="field-label">Type of change</label>
              <select value={form.change_type} onChange={(e) => setForm((p) => ({ ...p, change_type: e.target.value }))} className="field-select">
                {toArray(metadata.change_types).map((item) => <option key={item} value={item}>{prettyLabel(item)}</option>)}
              </select>
            </div>
          </div>

          <div>
            <label className="field-label">Petition title</label>
            <input
              value={form.title}
              onChange={(e) => setForm((p) => ({ ...p, title: e.target.value }))}
              maxLength={160}
              placeholder="Example: Add a PvE-only queue for solo players"
              className="field-input"
            />
          </div>

          <div>
            <label className="field-label">Short summary</label>
            <textarea
              value={form.summary}
              onChange={(e) => setForm((p) => ({ ...p, summary: e.target.value }))}
              rows={3}
              maxLength={300}
              placeholder="What change are you asking for, and why should players support it?"
              className="field-textarea"
            />
          </div>

          <div>
            <label className="field-label">Detailed request</label>
            <textarea
              value={form.body}
              onChange={(e) => setForm((p) => ({ ...p, body: e.target.value }))}
              rows={10}
              maxLength={8000}
              placeholder="Describe the current problem, who it affects, what should change, and what good looks like after the update."
              className="field-textarea"
            />
            <p className="mt-2 text-xs text-slate-500">
              Tip: include examples players will recognize and avoid bundling multiple unrelated requests into one petition.
            </p>
          </div>

          {error ? <p className="text-sm text-rose-700 bg-rose-50 border border-rose-100 rounded-xl px-3 py-2">{error}</p> : null}

          <div className="flex items-center justify-between gap-3 flex-wrap pt-2">
            <p className="text-xs text-slate-500">Petitions are public and shareable. Keep requests specific, respectful, and actionable.</p>
            <PrimaryButton type="submit" disabled={saving}>{saving ? 'Publishing…' : 'Publish Petition'}</PrimaryButton>
          </div>
        </form>
      </section>
    </main>
  );
}
