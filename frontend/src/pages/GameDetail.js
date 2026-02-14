import React, { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { AuthContext } from '../context/AuthContext';
import api from '../api/axios';

const STOP_WORDS = new Set([
  'the',
  'and',
  'with',
  'from',
  'this',
  'that',
  'have',
  'your',
  'about',
  'into',
  'they',
  'their',
  'them',
  'what',
  'when',
  'where',
  'which',
  'were',
  'been',
  'just',
  'also',
  'more',
  'some',
  'many',
  'over',
  'than',
  'there',
  'users',
  'community',
  'nike',
  'game',
  'reddit',
]);

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

const sentimentLevel = (label) => {
  const value = String(label || '').toLowerCase();
  if (value.includes('positive')) return 2;
  if (value.includes('negative')) return 0;
  return 1;
};

const asDate = (value) => {
  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? null : dt;
};

const formatDate = (value) => {
  const dt = asDate(value);
  return dt ? dt.toLocaleDateString() : 'Unknown';
};

const formatShortTime = (value) => {
  const dt = asDate(value);
  if (!dt) return 'Unknown';
  return `${dt.toLocaleDateString()}, ${dt.toLocaleTimeString()}`;
};

const toTokens = (value) => {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter((word) => word.length > 3 && !STOP_WORDS.has(word));
};

const findSourcePost = (entry, posts) => {
  const tokens = toTokens(entry);
  if (tokens.length === 0) return null;

  let best = null;
  let bestScore = 0;

  posts.forEach((post) => {
    const haystack = `${post?.title || ''} ${post?.selftext || ''}`.toLowerCase();
    let score = 0;

    tokens.forEach((token) => {
      if (haystack.includes(token)) score += 1;
    });

    if (score > bestScore) {
      bestScore = score;
      best = post;
    }
  });

  return bestScore > 0 ? best : null;
};

const MetricCard = ({ label, value, accent = '' }) => {
  return (
    <article className="card-glass p-5 min-h-[120px] flex flex-col justify-between">
      <p className="font-mono text-xs tracking-[0.2em] uppercase text-zinc-500">{label}</p>
      <p className={`font-heading text-4xl font-black leading-none ${accent}`}>{value}</p>
    </article>
  );
};

const InsightColumn = ({ title, icon, entries, sourcePosts, tone = 'neutral' }) => {
  const toneClass =
    tone === 'danger'
      ? 'text-[#FF4569]'
      : tone === 'success'
      ? 'text-[#7CFF9A]'
      : 'text-[#8BE8FF]';

  return (
    <article className="card-glass p-6">
      <h3 className="font-heading text-2xl font-bold flex items-center gap-3 mb-5">
        <span className={toneClass}>{icon}</span>
        <span>{title}</span>
      </h3>

      {entries.length === 0 ? (
        <p className="text-zinc-500 text-sm">None</p>
      ) : (
        <ol className="space-y-3">
          {entries.map((entry, idx) => {
            const source = findSourcePost(entry, sourcePosts);

            return (
              <li key={`${title}-${idx}`} className="text-zinc-200">
                <div className="flex gap-3">
                  <span className={`font-mono text-sm ${toneClass}`}>{String(idx + 1).padStart(2, '0')}</span>
                  <div>
                    <p className="text-lg leading-snug">{entry}</p>
                    {source?.permalink ? (
                      <a
                        href={source.permalink}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex mt-1 text-xs text-zinc-500 hover:text-zinc-300"
                      >
                        [source]
                      </a>
                    ) : null}
                  </div>
                </div>
              </li>
            );
          })}
        </ol>
      )}
    </article>
  );
};

const SentimentTrend = ({ history }) => {
  if (history.length === 0) {
    return (
      <section className="card-glass p-6">
        <div className="flex items-center justify-between mb-3">
          <h2 className="font-heading text-4xl font-black">Sentiment Trend</h2>
          <span className="font-mono text-sm text-zinc-500">0 scans</span>
        </div>
        <p className="text-zinc-400">Run more scans to build a trendline.</p>
      </section>
    );
  }

  const points = [...history].reverse();
  const width = 1000;
  const height = 240;
  const padX = 40;
  const padTop = 26;
  const padBottom = 36;
  const chartHeight = height - padTop - padBottom;
  const rowHeight = chartHeight / 2;

  const getY = (label) => {
    const level = sentimentLevel(label);
    return padTop + (2 - level) * rowHeight;
  };

  const getX = (idx) => {
    if (points.length === 1) return width / 2;
    return padX + (idx / (points.length - 1)) * (width - padX * 2);
  };

  const coords = points.map((item, idx) => ({
    x: getX(idx),
    y: getY(item.label),
    date: item.createdAt,
  }));

  const polylinePoints = coords.map((pt) => `${pt.x},${pt.y}`).join(' ');

  return (
    <section className="card-glass p-6">
      <div className="flex items-center justify-between mb-5">
        <h2 className="font-heading text-4xl font-black">Sentiment Trend</h2>
        <span className="font-mono text-sm text-zinc-500">{history.length} scans</span>
      </div>

      <div className="overflow-x-auto">
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full min-w-[680px] h-[240px]">
          <line x1={padX} y1={padTop} x2={width - padX} y2={padTop} stroke="rgba(255,255,255,0.14)" />
          <line x1={padX} y1={padTop + rowHeight} x2={width - padX} y2={padTop + rowHeight} stroke="rgba(255,255,255,0.10)" />
          <line x1={padX} y1={padTop + rowHeight * 2} x2={width - padX} y2={padTop + rowHeight * 2} stroke="rgba(255,255,255,0.14)" />

          <text x={12} y={padTop + 4} fill="rgba(255,255,255,0.45)" fontSize="14">POS</text>
          <text x={12} y={padTop + rowHeight + 4} fill="rgba(255,255,255,0.45)" fontSize="14">MIX</text>
          <text x={12} y={padTop + rowHeight * 2 + 4} fill="rgba(255,255,255,0.45)" fontSize="14">NEG</text>

          <polyline fill="none" stroke="#D3F34B" strokeWidth="3" points={polylinePoints} />

          {coords.map((pt, idx) => (
            <g key={`pt-${idx}`}>
              <circle cx={pt.x} cy={pt.y} r="5" fill="#D3F34B" />
              <text
                x={pt.x}
                y={height - 10}
                textAnchor="middle"
                fill="rgba(255,255,255,0.4)"
                fontSize="12"
              >
                {formatDate(pt.date)}
              </text>
            </g>
          ))}
        </svg>
      </div>

      <div className="flex gap-6 mt-3 font-mono text-sm">
        <span className="text-[#7CFF9A]">Positive</span>
        <span className="text-[#FCEE0A]">Mixed</span>
        <span className="text-[#FF4569]">Negative</span>
      </div>
    </section>
  );
};

const GameDetail = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const { logout } = useContext(AuthContext);

  const [game, setGame] = useState(null);
  const [latest, setLatest] = useState(null);
  const [latestDetail, setLatestDetail] = useState(null);
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

      try {
        const latestResp = await api.get(`/api/games/${id}/latest-result-detail`);
        setLatestDetail(latestResp.data || null);
      } catch (detailErr) {
        if (detailErr?.response?.status === 404) {
          setLatestDetail(null);
        } else {
          const detail = detailErr?.response?.data?.detail;
          setPageError(typeof detail === 'string' ? detail : 'Failed to load latest source posts.');
        }
      }
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

  const analysis = latestDetail?.analysis || latest?.analysis || {};
  const latestPosts = toArray(latestDetail?.posts);
  const latestComments = toArray(latestDetail?.comments);

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
        postsCount: Number(result?.posts_count || 0),
        commentsCount: Number(result?.comments_count || 0),
      };
    });
  }, [results]);

  const lastScanned = latestDetail?.created_at || latest?.created_at;
  const postsAnalyzed = latestPosts.length || Number(latest?.posts_count || 0);
  const commentsAnalyzed = latestComments.length || Number(latest?.comments_count || 0);

  const postsLast7Days = useMemo(() => {
    const now = Date.now();
    const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;

    return history.reduce((sum, item) => {
      const dt = asDate(item.createdAt);
      if (!dt) return sum;
      if (now - dt.getTime() > sevenDaysMs) return sum;
      return sum + item.postsCount;
    }, 0);
  }, [history]);

  const sourcePosts = useMemo(() => {
    const ranked = [...latestPosts];
    ranked.sort((a, b) => {
      const scoreDiff = Number(b?.score || 0) - Number(a?.score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      return Number(b?.created_utc || 0) - Number(a?.created_utc || 0);
    });
    return ranked.slice(0, 12);
  }, [latestPosts]);

  if (loading) {
    return <div className="min-h-screen flex items-center justify-center text-zinc-400">Loading game...</div>;
  }

  if (!game) {
    return (
      <div className="min-h-screen bg-[#09090b] px-6 py-10">
        <div className="max-w-5xl mx-auto card-glass p-8">
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
        <div className="max-w-[120rem] mx-auto px-4 md:px-8 py-4 flex items-center justify-between gap-3">
          <button
            onClick={() => navigate('/')}
            className="px-3 py-2 border border-white/15 text-sm text-zinc-300 hover:text-white hover:border-white/30"
          >
            Back to Dashboard
          </button>
          <div className="flex items-center gap-2">
            <button
              onClick={runScan}
              disabled={scanning}
              className="px-4 py-2 bg-[#00E5FF]/20 border border-[#00E5FF]/40 text-[#9CF5FF] disabled:opacity-60 text-sm"
            >
              {scanning ? 'Running...' : 'Run New Scan'}
            </button>
            <button
              onClick={deleteGame}
              disabled={deleting}
              className="px-4 py-2 bg-[#FF003C]/15 border border-[#FF003C]/40 text-[#ff8fa7] disabled:opacity-60 text-sm"
            >
              {deleting ? 'Deleting...' : 'Delete Game'}
            </button>
            <button
              onClick={logout}
              className="px-3 py-2 border border-white/15 text-sm text-zinc-300 hover:text-white hover:border-white/30"
            >
              Logout
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-[120rem] mx-auto px-4 md:px-8 py-8 space-y-6">
        <section className="card-glass p-6">
          <h1 className="font-heading text-4xl md:text-5xl font-black leading-none">{game.name}</h1>
          <p className="text-zinc-400 mt-2 text-lg">r/{game.subreddit}</p>
          {game.keywords ? <p className="text-sm text-zinc-500 mt-1">Keywords: {game.keywords}</p> : null}
          {pageError ? <p className="text-sm text-red-400 mt-4">{pageError}</p> : null}
        </section>

        <section className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-5 gap-4">
          <MetricCard label="Posts Analysed" value={postsAnalyzed} />
          <MetricCard label="Last 7 Days" value={postsLast7Days} />
          <MetricCard label="Comments" value={commentsAnalyzed} />
          <MetricCard
            label="Sentiment"
            value={analysis.sentiment_label || 'Unknown'}
            accent={String(analysis.sentiment_label || '').toLowerCase().includes('mixed') ? 'text-[#FCEE0A]' : ''}
          />
          <MetricCard label="Last Scanned" value={formatShortTime(lastScanned)} />
        </section>

        <section className="card-glass p-0 overflow-hidden">
          <div className="border-l-4 border-[#D3F34B] p-6 md:p-8">
            <h2 className="font-heading text-4xl font-black mb-4">Sentiment Analysis</h2>
            <div className={`inline-flex px-3 py-1 text-sm border ${sentimentStyles(analysis.sentiment_label)}`}>
              {analysis.sentiment_label || 'Unknown'}
            </div>
            <p className="text-zinc-100 mt-4 text-lg leading-relaxed whitespace-pre-wrap">
              {analysis.sentiment_summary || 'No summary was returned by analysis.'}
            </p>

            {analysis?.error ? (
              <details className="mt-4 text-sm text-zinc-400">
                <summary className="cursor-pointer hover:text-zinc-200">Debug Info</summary>
                <pre className="mt-2 p-3 bg-black/40 border border-white/10 overflow-auto text-xs whitespace-pre-wrap">
                  {JSON.stringify(analysis, null, 2)}
                </pre>
              </details>
            ) : null}
          </div>
        </section>

        <section className="grid grid-cols-1 xl:grid-cols-3 gap-4">
          <InsightColumn title="Top Themes" icon="TH" entries={themes} sourcePosts={sourcePosts} />
          <InsightColumn title="Pain Points" icon="PP" entries={painPoints} sourcePosts={sourcePosts} tone="danger" />
          <InsightColumn title="Community Wins" icon="CW" entries={wins} sourcePosts={sourcePosts} tone="success" />
        </section>

        <section className="card-glass p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-heading text-4xl font-black">Source Posts</h2>
            <span className="font-mono text-sm text-zinc-500">{latestPosts.length} posts</span>
          </div>

          {sourcePosts.length === 0 ? (
            <p className="text-zinc-400">No source posts available for the latest scan.</p>
          ) : (
            <div className="border border-white/10">
              {sourcePosts.map((post) => {
                const permalink = post?.permalink || (post?.id ? `https://www.reddit.com/comments/${post.id}/` : '');
                const dateText = post?.created_utc
                  ? new Date(Number(post.created_utc) * 1000).toLocaleDateString()
                  : 'Unknown date';

                return (
                  <article key={post.id || post.title} className="border-b border-white/10 last:border-b-0 p-5">
                    <div className="flex items-start justify-between gap-4">
                      <div>
                        <h3 className="text-2xl font-semibold leading-snug text-zinc-100">
                          {post.title || 'Untitled post'}
                        </h3>
                        <p className="font-mono text-sm text-zinc-500 mt-2">
                          {Number(post.score || 0)} pts  {Number(post.num_comments || 0)} comments  {dateText}
                        </p>
                      </div>
                      {permalink ? (
                        <a
                          href={permalink}
                          target="_blank"
                          rel="noreferrer"
                          className="text-zinc-500 hover:text-zinc-200 text-2xl"
                          aria-label="Open source post"
                          title="Open source post"
                        >open</a>
                      ) : null}
                    </div>
                  </article>
                );
              })}
            </div>
          )}
        </section>

        <SentimentTrend history={history} />

        <section className="card-glass p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-heading text-4xl font-black">Scan History</h2>
            <span className="font-mono text-sm text-zinc-500">{history.length} scans</span>
          </div>

          {history.length === 0 ? (
            <p className="text-zinc-400">No historical scans yet.</p>
          ) : (
            <div className="border border-white/10">
              {history.map((item) => (
                <article key={item.id} className="border-b border-white/10 last:border-b-0 px-5 py-4">
                  <div className="flex items-center justify-between gap-3">
                    <div className="flex items-center flex-wrap gap-x-4 gap-y-1">
                      <span className="font-mono text-zinc-500">{formatDate(item.createdAt)}</span>
                      <span className={`text-xl font-heading ${sentimentStyles(item.label)} border px-2 py-0.5`}>
                        {item.label}
                      </span>
                      <span className="font-mono text-zinc-500">
                        {item.postsCount} posts  {item.commentsCount} comments
                      </span>
                    </div>
                    <span className="text-zinc-500">></span>
                  </div>
                  {item.summary ? <p className="text-sm text-zinc-400 mt-2">{item.summary}</p> : null}
                </article>
              ))}
            </div>
          )}
        </section>
      </main>
    </div>
  );
};

export default GameDetail;



