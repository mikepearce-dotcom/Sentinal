import React, { useEffect, useState } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import api from '../api/axios';
import { GameBadge, PrimaryButton, Tag } from '../components/UI';
import { formatNumber, prettyLabel, toArray } from '../lib/community';
import { useAuth } from '../hooks/useAuth';

const TITLE_MIN = 8;
const SUMMARY_MIN = 12;
const BODY_MIN = 30;
const GAME_NAME_MIN = 2;

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
  const [touched, setTouched] = useState({ game: false, title: false, summary: false, body: false });

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

  const typedGameName = String(gameQuery || '').trim();
  const selectedGameName = String(selectedGame?.name || '').trim();
  const submitGameName = selectedGameName || typedGameName;
  const manualGameAllowed = !selectedGame && typedGameName.length >= GAME_NAME_MIN;

  const validation = {
    game: submitGameName.length >= GAME_NAME_MIN ? '' : `Enter or select a game name (${GAME_NAME_MIN}+ characters).`,
    title: form.title.trim().length >= TITLE_MIN ? '' : `Title must be at least ${TITLE_MIN} characters.`,
    summary: form.summary.trim().length >= SUMMARY_MIN ? '' : `Summary must be at least ${SUMMARY_MIN} characters.`,
    body: form.body.trim().length >= BODY_MIN ? '' : `Detailed request must be at least ${BODY_MIN} characters.`,
  };

  const hasValidationErrors = Object.values(validation).some(Boolean);
  const showFieldError = (field) => Boolean(touched[field] && validation[field]);

  const onSubmit = async (e) => {
    e.preventDefault();
    setTouched({ game: true, title: true, summary: true, body: true });
    setError('');
    if (hasValidationErrors) {
      setError('Please fix the highlighted fields before publishing.');
      return;
    }

    setSaving(true);
    try {
      const payload = {
        game_id: selectedGame?.id || '',
        game_name: submitGameName,
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
      if (Array.isArray(detail) && detail.length > 0) {
        const first = detail[0] || {};
        const fieldName = Array.isArray(first?.loc) ? String(first.loc[first.loc.length - 1] || '') : '';
        const message = String(first?.msg || 'Validation error');
        setError(fieldName ? `${fieldName}: ${message}` : message);
      } else {
        setError(typeof detail === 'string' ? detail : 'Failed to create petition.');
      }
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
              onBlur={() => setTouched((prev) => ({ ...prev, game: true }))}
              placeholder="Search a game (we check your studio catalog + IGDB)"
              className={`field-input ${showFieldError('game') ? 'border-rose-300 bg-rose-50/50' : ''}`}
            />
            <p className="mt-2 text-xs text-slate-500">
              We suggest games from your tracked catalog first, then IGDB to reduce typos. If your game is not listed, you can still use the name you typed.
            </p>
            {showFieldError('game') ? <p className="mt-2 text-xs text-rose-700">{validation.game}</p> : null}

            {selectedGame ? (
              <div className="mt-2 flex items-center gap-2 text-xs text-slate-500 flex-wrap">
                <Tag tone="success">Selected</Tag>
                <div className="inline-flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-2 py-1">
                  <GameBadge name={selectedGame.name} logoUrl={selectedGame.logo_url} size="sm" />
                  <span className="text-slate-700 font-medium">{selectedGame.name}</span>
                </div>
                <button type="button" className="copy-link text-xs" onClick={() => setSelectedGame(null)}>Clear</button>
              </div>
            ) : null}

            {manualGameAllowed ? (
              <div className="mt-2 flex items-center justify-between gap-2 rounded-xl border border-dashed border-slate-300 bg-white/70 px-3 py-2">
                <div className="min-w-0">
                  <p className="text-xs font-semibold text-slate-700">Can't find the game?</p>
                  <p className="text-xs text-slate-500 truncate">Use your typed name and create it manually: "{typedGameName}"</p>
                </div>
                <button
                  type="button"
                  className="btn-ghost px-3 py-1.5 text-xs font-semibold shrink-0"
                  onClick={() => {
                    setSelectedGame({ id: '', slug: '', name: typedGameName, logo_url: '', source: 'manual' });
                    setGameSuggestions([]);
                  }}
                >
                  Use typed name
                </button>
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
                      <div className="flex items-center gap-2 min-w-0">
                        <GameBadge name={game.name} logoUrl={game.logo_url} size="sm" />
                        <span className="text-sm text-slate-900 truncate">{game.name}</span>
                      </div>
                      <span className="text-xs text-slate-500 shrink-0">
                        {game.source === 'catalog' ? `${formatNumber(game.petition_count)} petitions` : game.source === 'igdb' ? 'IGDB match' : 'Suggested'}
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
            <div className="flex items-center justify-between gap-3">
              <label className="field-label mb-0">Petition title</label>
              <span className="text-xs text-slate-500">{form.title.length}/160</span>
            </div>
            <input
              value={form.title}
              onChange={(e) => setForm((p) => ({ ...p, title: e.target.value }))}
              onBlur={() => setTouched((prev) => ({ ...prev, title: true }))}
              maxLength={160}
              placeholder="Example: Add a PvE-only queue for solo players"
              className={`field-input ${showFieldError('title') ? 'border-rose-300 bg-rose-50/50' : ''}`}
            />
            {showFieldError('title') ? <p className="mt-2 text-xs text-rose-700">{validation.title}</p> : <p className="mt-2 text-xs text-slate-500">Be specific and focus on one change request.</p>}
          </div>

          <div>
            <div className="flex items-center justify-between gap-3">
              <label className="field-label mb-0">Short summary</label>
              <span className="text-xs text-slate-500">{form.summary.length}/300</span>
            </div>
            <textarea
              value={form.summary}
              onChange={(e) => setForm((p) => ({ ...p, summary: e.target.value }))}
              onBlur={() => setTouched((prev) => ({ ...prev, summary: true }))}
              rows={3}
              maxLength={300}
              placeholder="What change are you asking for, and why should players support it?"
              className={`field-textarea ${showFieldError('summary') ? 'border-rose-300 bg-rose-50/50' : ''}`}
            />
            {showFieldError('summary') ? <p className="mt-2 text-xs text-rose-700">{validation.summary}</p> : <p className="mt-2 text-xs text-slate-500">Give players a quick reason to support this request.</p>}
          </div>

          <div>
            <div className="flex items-center justify-between gap-3">
              <label className="field-label mb-0">Detailed request</label>
              <span className="text-xs text-slate-500">{form.body.length}/8000</span>
            </div>
            <textarea
              value={form.body}
              onChange={(e) => setForm((p) => ({ ...p, body: e.target.value }))}
              onBlur={() => setTouched((prev) => ({ ...prev, body: true }))}
              rows={10}
              maxLength={8000}
              placeholder="Describe the current problem, who it affects, what should change, and what good looks like after the update."
              className={`field-textarea ${showFieldError('body') ? 'border-rose-300 bg-rose-50/50' : ''}`}
            />
            {showFieldError('body') ? (
              <p className="mt-2 text-xs text-rose-700">{validation.body}</p>
            ) : (
              <p className="mt-2 text-xs text-slate-500">
                Tip: include examples players will recognize and avoid bundling multiple unrelated requests into one petition.
              </p>
            )}
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
