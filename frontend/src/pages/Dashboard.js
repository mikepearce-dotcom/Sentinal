import React, { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { AuthContext } from '../context/AuthContext';
import api from '../api/axios';

const Dashboard = () => {
  const navigate = useNavigate();
  const { user, logout } = useContext(AuthContext);

  const [games, setGames] = useState([]);
  const [loading, setLoading] = useState(true);
  const [loadingError, setLoadingError] = useState('');
  const [formError, setFormError] = useState('');
  const [creating, setCreating] = useState(false);
  const [runningScanFor, setRunningScanFor] = useState('');
  const [deletingGameFor, setDeletingGameFor] = useState('');
  const [newGame, setNewGame] = useState({ name: '', subreddit: '', keywords: '' });

  const sortedGames = useMemo(() => {
    return [...games].sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  }, [games]);

  const loadGames = useCallback(async () => {
    setLoading(true);
    setLoadingError('');

    try {
      const resp = await api.get('/api/games');
      setGames(resp.data || []);
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setLoadingError(typeof detail === 'string' ? detail : 'Failed to load tracked games.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadGames();
  }, [loadGames]);

  const addGame = async (e) => {
    e.preventDefault();
    setFormError('');

    if (!newGame.name.trim() || !newGame.subreddit.trim()) {
      setFormError('Game name and subreddit are required.');
      return;
    }

    setCreating(true);
    try {
      const payload = {
        name: newGame.name.trim(),
        subreddit: newGame.subreddit.trim().replace(/^r\//i, ''),
        keywords: newGame.keywords.trim(),
      };
      const resp = await api.post('/api/games', payload);
      setGames((prev) => [resp.data, ...prev]);
      setNewGame({ name: '', subreddit: '', keywords: '' });
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setFormError(typeof detail === 'string' ? detail : 'Failed to add game.');
    } finally {
      setCreating(false);
    }
  };

  const runScan = async (gameId) => {
    setRunningScanFor(gameId);
    try {
      await api.post(`/api/games/${gameId}/scan`);
      navigate(`/games/${gameId}`);
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setLoadingError(typeof detail === 'string' ? detail : 'Scan failed. Check OpenAI key and try again.');
    } finally {
      setRunningScanFor('');
    }
  };

  const deleteGame = async (gameId) => {
    const confirmed = window.confirm('Delete this game and its results?');
    if (!confirmed) return;

    setDeletingGameFor(gameId);
    try {
      await api.delete(`/api/games/${gameId}`);
      setGames((prev) => prev.filter((g) => g.id !== gameId));
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setLoadingError(typeof detail === 'string' ? detail : 'Failed to delete game.');
    } finally {
      setDeletingGameFor('');
    }
  };

  if (!user) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-[#09090b] text-zinc-400">
        Loading user...
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#09090b]">
      <header className="border-b border-white/5">
        <div className="max-w-6xl mx-auto px-6 md:px-10 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 bg-[#D3F34B] text-black font-bold flex items-center justify-center">S</div>
            <div>
              <p className="font-heading font-black tracking-wide">SENTIENT TRACKER</p>
              <p className="text-xs text-zinc-500">Signed in as {user.email}</p>
            </div>
          </div>
          <button
            onClick={logout}
            className="px-4 py-2 border border-white/10 text-zinc-300 hover:text-white hover:border-white/30 transition-colors"
          >
            Logout
          </button>
        </div>
      </header>

      <section className="hero-glow border-b border-white/5">
        <div className="max-w-6xl mx-auto px-6 md:px-10 pt-12 pb-10">
          <h1 className="font-heading text-4xl md:text-5xl font-black leading-none tracking-tight">Game Sentiment Radar</h1>
          <p className="mt-4 max-w-2xl text-zinc-400">
            Track game communities on Reddit, run AI scans, and review sentiment themes in one dashboard.
          </p>
        </div>
      </section>

      <main className="max-w-6xl mx-auto px-6 md:px-10 py-8 space-y-8">
        <section className="card-glass p-6">
          <h2 className="font-heading text-xl font-bold mb-4">Add New Game</h2>
          <form onSubmit={addGame} className="grid grid-cols-1 md:grid-cols-4 gap-3">
            <input
              type="text"
              placeholder="Game name"
              value={newGame.name}
              onChange={(e) => setNewGame((prev) => ({ ...prev, name: e.target.value }))}
              className="md:col-span-1 w-full p-3 bg-black/40 border border-white/10 rounded text-white"
            />
            <input
              type="text"
              placeholder="Subreddit (e.g. Eldenring)"
              value={newGame.subreddit}
              onChange={(e) => setNewGame((prev) => ({ ...prev, subreddit: e.target.value }))}
              className="md:col-span-1 w-full p-3 bg-black/40 border border-white/10 rounded text-white"
            />
            <input
              type="text"
              placeholder="Keywords (optional)"
              value={newGame.keywords}
              onChange={(e) => setNewGame((prev) => ({ ...prev, keywords: e.target.value }))}
              className="md:col-span-1 w-full p-3 bg-black/40 border border-white/10 rounded text-white"
            />
            <button
              type="submit"
              disabled={creating}
              className="btn-primary md:col-span-1 px-4 py-3 disabled:opacity-60"
            >
              <span>{creating ? 'ADDING...' : 'ADD GAME'}</span>
            </button>
          </form>
          {formError ? <p className="text-sm text-red-400 mt-3">{formError}</p> : null}
        </section>

        <section>
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-heading text-xl font-bold">Tracked Games</h2>
            <span className="font-mono text-xs text-zinc-500">{sortedGames.length} total</span>
          </div>

          {loading ? (
            <div className="card-glass p-8 text-zinc-400">Loading games...</div>
          ) : loadingError ? (
            <div className="card-glass p-8 text-red-400">{loadingError}</div>
          ) : sortedGames.length === 0 ? (
            <div className="card-glass p-10 text-center">
              <h3 className="font-heading text-lg font-bold">No games tracked yet</h3>
              <p className="mt-2 text-zinc-400">Add a game above to start scanning Reddit sentiment.</p>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-5">
              {sortedGames.map((game) => {
                const scanning = runningScanFor === game.id;
                const deleting = deletingGameFor === game.id;

                return (
                  <article key={game.id} className="card-glass card-hover p-5">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <h3 className="font-heading text-xl font-bold break-words">{game.name}</h3>
                        <p className="text-zinc-400 text-sm mt-1">r/{game.subreddit}</p>
                        {game.keywords ? (
                          <p className="text-xs text-zinc-500 mt-2">Keywords: {game.keywords}</p>
                        ) : null}
                      </div>
                      <span className="font-mono text-[10px] text-zinc-500 whitespace-nowrap">
                        {new Date(game.created_at).toLocaleDateString()}
                      </span>
                    </div>

                    <div className="mt-5 grid grid-cols-3 gap-2">
                      <button
                        onClick={() => navigate(`/games/${game.id}`)}
                        className="col-span-1 px-3 py-2 border border-white/15 text-sm hover:border-white/30 transition-colors"
                      >
                        Details
                      </button>
                      <button
                        onClick={() => runScan(game.id)}
                        disabled={scanning}
                        className="col-span-1 px-3 py-2 bg-[#00E5FF]/20 border border-[#00E5FF]/40 text-[#9CF5FF] text-sm disabled:opacity-60"
                      >
                        {scanning ? 'Scanning...' : 'Run Scan'}
                      </button>
                      <button
                        onClick={() => deleteGame(game.id)}
                        disabled={deleting}
                        className="col-span-1 px-3 py-2 bg-[#FF003C]/15 border border-[#FF003C]/40 text-[#ff8fa7] text-sm disabled:opacity-60"
                      >
                        {deleting ? 'Deleting...' : 'Delete'}
                      </button>
                    </div>
                  </article>
                );
              })}
            </div>
          )}
        </section>
      </main>
    </div>
  );
};

export default Dashboard;
