import React, { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { AuthContext } from '../context/AuthContext';
import api from '../api/axios';

const toArray = (value) => (Array.isArray(value) ? value : []);

const normalizeListEntry = (entry) => {
  if (entry == null) return '';
  if (typeof entry === 'string') return entry;
  if (typeof entry === 'object') {
    if (typeof entry.title === 'string') return entry.title;
    if (typeof entry.point === 'string') return entry.point;
    if (typeof entry.summary === 'string') return entry.summary;
    return JSON.stringify(entry);
  }
  return String(entry);
};

const sentimentStyles = (label) => {
  const value = String(label || '').toLowerCase();
  if (value.includes('positive')) return 'text-[#00FF94] border-[#00FF94]/40 bg-[#00FF94]/10';
  if (value.includes('negative')) return 'text-[#FF003C] border-[#FF003C]/40 bg-[#FF003C]/10';
  if (value.includes('mixed')) return 'text-[#FCEE0A] border-[#FCEE0A]/40 bg-[#FCEE0A]/10';
  return 'text-zinc-300 border-white/15 bg-white/5';
};

const GameDetail = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const { logout } = useContext(AuthContext);

  const [game, setGame] = useState(null);
  const [latest, setLatest] = useState(null);
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(true);
  const [scanning, setScanning] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [pageError, setPageError] = useState('');

  const loadPage = useCallback(async () => {
    setLoading(true);
    setPageError('');

    try {
      const [gameResp, resultsResp] = await Promise.all([
        api.get(`/api/games/${id}`),
        api.get(`/api/games/${id}/results`),
      ]);

      const sorted = [...(resultsResp.data || [])].sort(
        (a, b) => new Date(b.created_at) - new Date(a.created_at)
      );

      setGame(gameResp.data);
      setResults(sorted);
      setLatest(sorted[0] || null);
    } catch (err) {
      if (err?.response?.status === 404) {
        setPageError('Game not found.');
      } else {
        const detail = err?.response?.data?.detail;
        setPageError(typeof detail === 'string' ? detail : 'Failed to load game details.');
      }
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    loadPage();
  }, [loadPage]);

  const runScan = async () => {
    setScanning(true);
    setPageError('');

    try {
      await api.post(`/api/games/${id}/scan`);
      await loadPage();
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setPageError(typeof detail === 'string' ? detail : 'Scan failed. Check backend logs/OpenAI key.');
    } finally {
      setScanning(false);
    }
  };

  const deleteGame = async () => {
    const confirmed = window.confirm('Delete this game and all scan history?');
    if (!confirmed) return;

    setDeleting(true);
    try {
      await api.delete(`/api/games/${id}`);
      navigate('/');
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setPageError(typeof detail === 'string' ? detail : 'Failed to delete game.');
    } finally {
      setDeleting(false);
    }
  };

  const analysis = latest?.analysis || {};
  const themes = toArray(analysis.themes).map(normalizeListEntry).filter(Boolean);
  const painPoints = toArray(analysis.pain_points).map(normalizeListEntry).filter(Boolean);
  const wins = toArray(analysis.wins).map(normalizeListEntry).filter(Boolean);

  const history = useMemo(() => {
    return results.map((result) => {
      const label = result?.analysis?.sentiment_label || 'Unknown';
      return {
        id: result.id,
        createdAt: result.created_at,
        label,
        summary: result?.analysis?.sentiment_summary || '',
      };
    });
  }, [results]);

  if (loading) {
    return <div className="min-h-screen flex items-center justify-center text-zinc-400">Loading game...</div>;
  }

  if (!game) {
    return (
      <div className="min-h-screen bg-[#09090b] px-6 py-10">
        <div className="max-w-4xl mx-auto card-glass p-8">
          <p className="text-red-400">{pageError || 'Unable to load this game.'}</p>
          <button
            onClick={() => navigate('/')}
            className="mt-4 px-4 py-2 border border-white/15 text-zinc-300 hover:text-white"
          >
            Back to Dashboard
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#09090b]">
      <header className="border-b border-white/5">
        <div className="max-w-6xl mx-auto px-6 md:px-10 py-4 flex items-center justify-between">
          <button
            onClick={() => navigate('/')}
            className="px-3 py-2 border border-white/15 text-sm text-zinc-300 hover:text-white hover:border-white/30"
          >
            Back to Dashboard
          </button>
          <button
            onClick={logout}
            className="px-3 py-2 border border-white/15 text-sm text-zinc-300 hover:text-white hover:border-white/30"
          >
            Logout
          </button>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-6 md:px-10 py-8 space-y-6">
        <section className="card-glass p-6">
          <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
            <div>
              <h1 className="font-heading text-4xl font-black leading-none">{game.name}</h1>
              <p className="text-zinc-400 mt-2">r/{game.subreddit}</p>
              {game.keywords ? <p className="text-xs text-zinc-500 mt-1">Keywords: {game.keywords}</p> : null}
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                onClick={runScan}
                disabled={scanning}
                className="px-4 py-2 bg-[#00E5FF]/20 border border-[#00E5FF]/40 text-[#9CF5FF] disabled:opacity-60"
              >
                {scanning ? 'Running Scan...' : 'Run New Scan'}
              </button>
              <button
                onClick={deleteGame}
                disabled={deleting}
                className="px-4 py-2 bg-[#FF003C]/15 border border-[#FF003C]/40 text-[#ff8fa7] disabled:opacity-60"
              >
                {deleting ? 'Deleting...' : 'Delete Game'}
              </button>
            </div>
          </div>
          {pageError ? <p className="text-sm text-red-400 mt-4">{pageError}</p> : null}
        </section>

        <section className="card-glass p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-heading text-2xl font-bold">Latest Analysis</h2>
            {latest ? (
              <span className="font-mono text-xs text-zinc-500">
                {new Date(latest.created_at).toLocaleString()}
              </span>
            ) : null}
          </div>

          {!latest ? (
            <p className="text-zinc-400">No scan results yet. Run your first scan.</p>
          ) : (
            <div className="space-y-5">
              <div className={`inline-flex px-3 py-1 text-sm border ${sentimentStyles(analysis.sentiment_label)}`}>
                {analysis.sentiment_label || 'Unknown'}
              </div>

              <div>
                <h3 className="text-sm uppercase tracking-wide text-zinc-400">Summary</h3>
                <p className="text-zinc-100 mt-2 whitespace-pre-wrap">
                  {analysis.sentiment_summary || 'No summary was returned by analysis.'}
                </p>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <h4 className="text-sm uppercase tracking-wide text-zinc-400 mb-2">Themes</h4>
                  {themes.length === 0 ? (
                    <p className="text-zinc-500 text-sm">None</p>
                  ) : (
                    <ul className="space-y-2">
                      {themes.map((item, idx) => (
                        <li key={`theme-${idx}`} className="text-sm text-zinc-200 border border-white/10 bg-black/30 p-2">
                          {item}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>

                <div>
                  <h4 className="text-sm uppercase tracking-wide text-zinc-400 mb-2">Pain Points</h4>
                  {painPoints.length === 0 ? (
                    <p className="text-zinc-500 text-sm">None</p>
                  ) : (
                    <ul className="space-y-2">
                      {painPoints.map((item, idx) => (
                        <li key={`pain-${idx}`} className="text-sm text-zinc-200 border border-white/10 bg-black/30 p-2">
                          {item}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>

                <div>
                  <h4 className="text-sm uppercase tracking-wide text-zinc-400 mb-2">Wins</h4>
                  {wins.length === 0 ? (
                    <p className="text-zinc-500 text-sm">None</p>
                  ) : (
                    <ul className="space-y-2">
                      {wins.map((item, idx) => (
                        <li key={`win-${idx}`} className="text-sm text-zinc-200 border border-white/10 bg-black/30 p-2">
                          {item}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </div>
            </div>
          )}
        </section>

        <section className="card-glass p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-heading text-2xl font-bold">Scan History</h2>
            <span className="font-mono text-xs text-zinc-500">{history.length} scans</span>
          </div>

          {history.length === 0 ? (
            <p className="text-zinc-400">No historical scans yet.</p>
          ) : (
            <div className="space-y-3">
              {history.map((item) => (
                <div key={item.id} className="border border-white/10 bg-black/30 p-3">
                  <div className="flex items-center justify-between gap-4">
                    <span className={`text-sm px-2 py-1 border ${sentimentStyles(item.label)}`}>{item.label}</span>
                    <span className="font-mono text-xs text-zinc-500">{new Date(item.createdAt).toLocaleString()}</span>
                  </div>
                  {item.summary ? <p className="text-sm text-zinc-300 mt-2">{item.summary}</p> : null}
                </div>
              ))}
            </div>
          )}
        </section>
      </main>
    </div>
  );
};

export default GameDetail;
