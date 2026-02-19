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
    if (typeof entry.text === 'string') return entry.text;
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

const POST_REF_REGEX = /\[POST:([a-z0-9_]+)\]/gi;

const toTokens = (value) => {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter((word) => word.length > 3 && !STOP_WORDS.has(word));
};

const extractPostIdsFromText = (value) => {
  const ids = [];
  const text = String(value || '');
  if (!text) return ids;

  let match;
  while ((match = POST_REF_REGEX.exec(text)) !== null) {
    const id = String(match[1] || '').trim();
    if (id && !ids.includes(id)) ids.push(id);
  }
  POST_REF_REGEX.lastIndex = 0;

  return ids;
};

const buildPostLookup = (posts) => {
  const byId = {};
  toArray(posts).forEach((post) => {
    const id = String(post?.id || '').trim();
    if (!id) return;
    byId[id] = post;
  });
  return byId;
};

const toPermalink = (postId, postsById) => {
  const id = String(postId || '').trim();
  if (!id) return '';
  const fromPost = postsById?.[id]?.permalink;
  return fromPost || `https://www.reddit.com/comments/${id}/`;
};

const normalizeEvidenceLinks = (value) => {
  const links = [];

  const pushLink = (raw) => {
    const candidate = String(raw || '').trim();
    if (!candidate) return;

    const match = candidate.match(/reddit\.com\/comments\/([a-z0-9_]+)/i);
    if (match) {
      const canonical = `https://www.reddit.com/comments/${match[1]}/`;
      if (!links.includes(canonical)) links.push(canonical);
      return;
    }

    if (/^https?:\/\//i.test(candidate) && !links.includes(candidate)) {
      links.push(candidate);
    }
  };

  if (Array.isArray(value)) {
    value.forEach(pushLink);
  } else {
    pushLink(value);
  }

  return links;
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

const normalizeInsightEntries = (entries, postsById) => {
  return toArray(entries)
    .map((entry) => {
      const text = normalizeListEntry(entry);
      const postIds = extractPostIdsFromText(text);

      const fromIds = postIds
        .map((postId) => toPermalink(postId, postsById))
        .filter(Boolean);

      const fromEvidence = entry && typeof entry === 'object'
        ? normalizeEvidenceLinks(entry.evidence)
        : [];

      const evidenceLinks = [];
      [...fromIds, ...fromEvidence].forEach((link) => {
        if (!evidenceLinks.includes(link)) evidenceLinks.push(link);
      });

      return {
        text,
        evidenceLinks,
      };
    })
    .filter((item) => Boolean(item.text));
};

const renderTextWithPostRefs = (value, postsById) => {
  const content = String(value || '');
  if (!content) return content;

  const nodes = [];
  let lastIndex = 0;
  let match;

  while ((match = POST_REF_REGEX.exec(content)) !== null) {
    const start = match.index;
    const end = POST_REF_REGEX.lastIndex;

    if (start > lastIndex) {
      nodes.push(content.slice(lastIndex, start));
    }

    const postId = String(match[1] || '').trim();
    const href = toPermalink(postId, postsById);

    if (href) {
      nodes.push(
        <a
          key={`post-ref-${start}-${postId}`}
          href={href}
          target="_blank"
          rel="noreferrer"
          className="text-[#D3F34B] hover:text-[#e7ff8b] underline decoration-transparent hover:decoration-current"
        >
          [POST:{postId}]
        </a>
      );
    } else {
      nodes.push(match[0]);
    }

    lastIndex = end;
  }

  if (lastIndex < content.length) {
    nodes.push(content.slice(lastIndex));
  }

  POST_REF_REGEX.lastIndex = 0;
  return nodes.length > 0 ? nodes : content;
};

const MetricCard = ({ label, value, accent = '' }) => {
  return (
    <article className="card-glass p-5 min-h-[120px] flex flex-col justify-between">
      <p className="font-mono text-xs tracking-[0.2em] uppercase text-zinc-500">{label}</p>
      <p className={`font-heading text-4xl font-black leading-none ${accent}`}>{value}</p>
    </article>
  );
};

const InsightColumn = ({ title, icon, entries, sourcePosts, postsById, tone = 'neutral' }) => {
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
            const fallbackSource = entry.evidenceLinks.length === 0
              ? findSourcePost(entry.text, sourcePosts)
              : null;

            const sourceLinks = entry.evidenceLinks.length > 0
              ? entry.evidenceLinks.slice(0, 2)
              : fallbackSource?.permalink
                ? [fallbackSource.permalink]
                : [];

            return (
              <li key={`${title}-${idx}`} className="text-zinc-200">
                <div className="flex gap-3">
                  <span className={`font-mono text-sm ${toneClass}`}>{String(idx + 1).padStart(2, '0')}</span>
                  <div>
                    <p className="text-lg leading-snug">{renderTextWithPostRefs(entry.text, postsById)}</p>
                    {sourceLinks.length > 0 ? (
                      <div className="mt-1 flex items-center gap-3">
                        {sourceLinks.map((link, sourceIdx) => (
                          <a
                            key={`${title}-${idx}-source-${sourceIdx}`}
                            href={link}
                            target="_blank"
                            rel="noreferrer"
                            className="inline-flex text-xs text-zinc-500 hover:text-zinc-300"
                          >
                            [source {sourceIdx + 1}]
                          </a>
                        ))}
                      </div>
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
  const [selectedSubreddits, setSelectedSubreddits] = useState([]);
  const [communitySuggestions, setCommunitySuggestions] = useState([]);
  const [discoveringCommunities, setDiscoveringCommunities] = useState(false);
  const [multiScanning, setMultiScanning] = useState(false);
  const [multiScanError, setMultiScanError] = useState('');
  const [multiScanResult, setMultiScanResult] = useState(null);

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

      if (sorted.length === 0) {
        setLatestDetail(null);
      } else {
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

  useEffect(() => {
    const baseSubreddit = String(game?.subreddit || '').replace(/^r\//i, '').trim();
    if (!baseSubreddit) return;

    setSelectedSubreddits((prev) => {
      const existing = prev.map((item) => String(item || '').toLowerCase());
      if (existing.includes(baseSubreddit.toLowerCase())) return prev;
      return [baseSubreddit, ...prev].slice(0, 5);
    });
  }, [game?.subreddit]);

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

  const addSelectedSubreddit = (subredditInput) => {
    const subreddit = String(subredditInput || '').replace(/^r\//i, '').trim();
    if (!subreddit) return;

    setSelectedSubreddits((prev) => {
      const existing = prev.map((item) => String(item || '').toLowerCase());
      if (existing.includes(subreddit.toLowerCase())) return prev;
      if (prev.length >= 5) return prev;
      return [...prev, subreddit];
    });
  };

  const removeSelectedSubreddit = (subredditInput) => {
    const subreddit = String(subredditInput || '').toLowerCase();
    if (!subreddit) return;

    setSelectedSubreddits((prev) => {
      if (prev.length <= 1) return prev;
      return prev.filter((item) => String(item || '').toLowerCase() !== subreddit);
    });
  };

  const discoverCommunities = async () => {
    const gameName = String(game?.name || '').trim();
    if (!gameName) {
      setMultiScanError('Game name is required to discover communities.');
      return;
    }

    setDiscoveringCommunities(true);
    setMultiScanError('');

    try {
      const resp = await api.get('/api/games/discover-subreddits', {
        params: {
          game_name: gameName,
          max_results: 5,
        },
      });

      const suggestions = Array.isArray(resp?.data?.results) ? resp.data.results : [];
      setCommunitySuggestions(suggestions);

      suggestions.forEach((item) => {
        const subreddit = String(item?.subreddit || '').trim();
        if (subreddit) addSelectedSubreddit(subreddit);
      });

      if (suggestions.length === 0) {
        setMultiScanError('No additional subreddit suggestions found for this game.');
      }
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setMultiScanError(typeof detail === 'string' ? detail : 'Failed to discover related communities.');
    } finally {
      setDiscoveringCommunities(false);
    }
  };

  const runMultiScan = async () => {
    if (selectedSubreddits.length === 0) {
      setMultiScanError('Select at least one subreddit for a combined scan.');
      return;
    }

    setMultiScanning(true);
    setMultiScanError('');

    try {
      const payload = {
        subreddits: selectedSubreddits,
        game_name: String(game?.name || ''),
        keywords: String(game?.keywords || ''),
        include_breakdown: true,
      };

      const resp = await api.post('/api/games/multi-scan', payload);
      setMultiScanResult(resp?.data || null);
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setMultiScanError(typeof detail === 'string' ? detail : 'Combined scan failed.');
    } finally {
      setMultiScanning(false);
    }
  };

  const analysis = latestDetail?.analysis || latest?.analysis || {};
  const latestPosts = toArray(latestDetail?.posts);
  const latestComments = toArray(latestDetail?.comments);

  const sourcePostsById = useMemo(() => buildPostLookup(latestPosts), [latestPosts]);

  const themes = useMemo(
    () => normalizeInsightEntries(analysis.themes, sourcePostsById),
    [analysis.themes, sourcePostsById]
  );
  const painPoints = useMemo(
    () => normalizeInsightEntries(analysis.pain_points, sourcePostsById),
    [analysis.pain_points, sourcePostsById]
  );
  const wins = useMemo(
    () => normalizeInsightEntries(analysis.wins, sourcePostsById),
    [analysis.wins, sourcePostsById]
  );

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

  const multiOverall = multiScanResult?.overall || {};
  const multiMeta = multiScanResult?.meta || {};
  const multiBreakdownRows = toArray(multiScanResult?.subreddit_breakdown?.breakdown);

  const multiOverallThemes = useMemo(
    () => normalizeInsightEntries(multiOverall?.themes, {}),
    [multiOverall?.themes]
  );
  const multiOverallPain = useMemo(
    () => normalizeInsightEntries(multiOverall?.pain_points, {}),
    [multiOverall?.pain_points]
  );
  const multiOverallWins = useMemo(
    () => normalizeInsightEntries(multiOverall?.wins, {}),
    [multiOverall?.wins]
  );

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

        <section className="card-glass p-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h2 className="font-heading text-3xl font-black">Combined Community Scan</h2>
            <span className="font-mono text-xs text-zinc-500">{selectedSubreddits.length}/5 selected</span>
          </div>
          <p className="mt-2 text-zinc-400">
            Run one scan across multiple subreddits. Overall analysis is shown first, with a per-community breakdown below.
          </p>

          <div className="mt-4 flex flex-wrap gap-2">
            {selectedSubreddits.map((subreddit) => {
              const normalized = String(subreddit || '').trim();
              const isPrimary = normalized.toLowerCase() === String(game?.subreddit || '').replace(/^r\//i, '').toLowerCase();
              return (
                <span
                  key={`selected-${normalized}`}
                  className="inline-flex items-center gap-2 px-3 py-1 border border-[#D3F34B]/30 bg-[#D3F34B]/10 text-[#e7ff8b]"
                >
                  r/{normalized}
                  {!isPrimary && selectedSubreddits.length > 1 ? (
                    <button
                      type="button"
                      onClick={() => removeSelectedSubreddit(normalized)}
                      className="text-[#e7ff8b] hover:text-white text-xs"
                      title="Remove subreddit"
                    >
                      x
                    </button>
                  ) : null}
                </span>
              );
            })}
          </div>

          <div className="mt-4 flex flex-wrap gap-2">
            <button
              type="button"
              onClick={discoverCommunities}
              disabled={discoveringCommunities}
              className="px-4 py-2 border border-white/15 text-zinc-300 hover:text-white hover:border-white/30 disabled:opacity-60"
            >
              {discoveringCommunities ? 'Discovering...' : 'Discover Communities'}
            </button>
            <button
              type="button"
              onClick={runMultiScan}
              disabled={multiScanning || selectedSubreddits.length === 0}
              className="px-4 py-2 bg-[#D3F34B]/20 border border-[#D3F34B]/40 text-[#e7ff8b] disabled:opacity-60"
            >
              {multiScanning ? 'Running Combined Scan...' : 'Run Combined Scan'}
            </button>
          </div>

          {communitySuggestions.length > 0 ? (
            <div className="mt-4 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-2">
              {communitySuggestions.map((item, idx) => {
                const subreddit = String(item?.subreddit || '').trim();
                if (!subreddit) return null;

                const alreadySelected = selectedSubreddits.some(
                  (value) => String(value || '').toLowerCase() === subreddit.toLowerCase()
                );

                return (
                  <button
                    key={`community-${subreddit}-${idx}`}
                    type="button"
                    onClick={() => addSelectedSubreddit(subreddit)}
                    disabled={alreadySelected || selectedSubreddits.length >= 5}
                    className={`text-left p-3 border transition-colors ${
                      alreadySelected
                        ? 'border-[#D3F34B]/40 bg-[#D3F34B]/10 text-[#e7ff8b]'
                        : 'border-white/10 bg-black/30 text-zinc-200 hover:border-white/30'
                    } disabled:opacity-60`}
                  >
                    <p className="font-semibold">r/{subreddit}</p>
                    <p className="text-xs text-zinc-400 mt-1">{item?.reason || 'Relevant community match.'}</p>
                  </button>
                );
              })}
            </div>
          ) : null}

          {multiScanError ? <p className="text-sm text-amber-300 mt-3">{multiScanError}</p> : null}
        </section>

        {multiScanResult ? (
          <>
            <section className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
              <MetricCard label="Combined Posts" value={Number(multiMeta?.posts_analysed || 0)} />
              <MetricCard label="Combined Comments" value={Number(multiMeta?.comments_sampled || 0)} />
              <MetricCard label="Communities" value={toArray(multiMeta?.subreddits).length} />
              <MetricCard
                label="Overall Sentiment"
                value={multiOverall?.sentiment_label || 'Unknown'}
                accent={String(multiOverall?.sentiment_label || '').toLowerCase().includes('mixed') ? 'text-[#FCEE0A]' : ''}
              />
            </section>

            <section className="card-glass p-0 overflow-hidden">
              <div className="border-l-4 border-[#00E5FF] p-6 md:p-8">
                <h2 className="font-heading text-4xl font-black mb-4">Combined Sentiment Analysis</h2>
                <div className={`inline-flex px-3 py-1 text-sm border ${sentimentStyles(multiOverall?.sentiment_label)}`}>
                  {multiOverall?.sentiment_label || 'Unknown'}
                </div>
                <p className="text-zinc-100 mt-4 text-lg leading-relaxed whitespace-pre-wrap">
                  {renderTextWithPostRefs(
                    multiOverall?.sentiment_summary || 'No combined summary was returned by analysis.',
                    {}
                  )}
                </p>
              </div>
            </section>

            <section className="grid grid-cols-1 xl:grid-cols-3 gap-4">
              <InsightColumn
                title="Combined Themes"
                icon="TH"
                entries={multiOverallThemes}
                sourcePosts={[]}
                postsById={{}}
              />
              <InsightColumn
                title="Combined Pain Points"
                icon="PP"
                entries={multiOverallPain}
                sourcePosts={[]}
                postsById={{}}
                tone="danger"
              />
              <InsightColumn
                title="Combined Wins"
                icon="CW"
                entries={multiOverallWins}
                sourcePosts={[]}
                postsById={{}}
                tone="success"
              />
            </section>

            <section className="card-glass p-6">
              <div className="flex items-center justify-between mb-4">
                <h2 className="font-heading text-4xl font-black">Per-Subreddit Breakdown</h2>
                <span className="font-mono text-sm text-zinc-500">{multiBreakdownRows.length} communities</span>
              </div>

              {multiScanResult?.subreddit_breakdown?.error ? (
                <p className="text-sm text-amber-300 mb-4">
                  Using deterministic subreddit breakdown fallback while AI formatting is unavailable.
                </p>
              ) : null}

              {multiBreakdownRows.length === 0 ? (
                <p className="text-zinc-400">No subreddit-level breakdown was returned.</p>
              ) : (
                <div className="space-y-4">
                  {multiBreakdownRows.map((row, idx) => {
                    const rowThemes = toArray(row?.top_themes).map(normalizeListEntry).filter(Boolean).slice(0, 5);
                    const rowPain = normalizeInsightEntries(row?.top_pain_points, {});
                    const rowWins = normalizeInsightEntries(row?.top_wins, {});
                    const rowBullets = toArray(row?.summary_bullets).map(normalizeListEntry).filter(Boolean).slice(0, 3);

                    return (
                      <article key={`breakdown-${row?.subreddit || idx}`} className="border border-white/10 bg-black/20 p-5">
                        <div className="flex items-center justify-between gap-3">
                          <h3 className="font-heading text-2xl font-bold">r/{row?.subreddit || 'unknown'}</h3>
                          <span className={`text-sm border px-2 py-1 ${sentimentStyles(row?.sentiment_label)}`}>
                            {row?.sentiment_label || 'Unknown'}
                          </span>
                        </div>

                        {rowBullets.length > 0 ? (
                          <ul className="mt-3 space-y-1 text-zinc-200">
                            {rowBullets.map((bullet, bulletIdx) => (
                              <li key={`bullet-${idx}-${bulletIdx}`}>
                                {renderTextWithPostRefs(bullet, {})}
                              </li>
                            ))}
                          </ul>
                        ) : null}

                        <div className="mt-4 grid grid-cols-1 xl:grid-cols-3 gap-4">
                          <div>
                            <h4 className="font-heading text-lg font-bold mb-2 text-[#8BE8FF]">Top Themes</h4>
                            {rowThemes.length === 0 ? (
                              <p className="text-zinc-500 text-sm">None</p>
                            ) : (
                              <ul className="space-y-1 text-zinc-200">
                                {rowThemes.map((theme, themeIdx) => (
                                  <li key={`theme-${idx}-${themeIdx}`}>
                                    {renderTextWithPostRefs(theme, {})}
                                  </li>
                                ))}
                              </ul>
                            )}
                          </div>

                          <div>
                            <h4 className="font-heading text-lg font-bold mb-2 text-[#FF4569]">Top Pain Points</h4>
                            {rowPain.length === 0 ? (
                              <p className="text-zinc-500 text-sm">None</p>
                            ) : (
                              <ul className="space-y-2 text-zinc-200">
                                {rowPain.map((item, painIdx) => (
                                  <li key={`pain-${idx}-${painIdx}`}>
                                    <p>{renderTextWithPostRefs(item.text, {})}</p>
                                    <div className="mt-1 flex flex-wrap gap-3">
                                      {toArray(item.evidenceLinks).map((link, linkIdx) => (
                                        <a
                                          key={`pain-link-${idx}-${painIdx}-${linkIdx}`}
                                          href={link}
                                          target="_blank"
                                          rel="noreferrer"
                                          className="text-xs text-zinc-500 hover:text-zinc-300"
                                        >
                                          [source {linkIdx + 1}]
                                        </a>
                                      ))}
                                    </div>
                                  </li>
                                ))}
                              </ul>
                            )}
                          </div>

                          <div>
                            <h4 className="font-heading text-lg font-bold mb-2 text-[#7CFF9A]">Top Wins</h4>
                            {rowWins.length === 0 ? (
                              <p className="text-zinc-500 text-sm">None</p>
                            ) : (
                              <ul className="space-y-2 text-zinc-200">
                                {rowWins.map((item, winIdx) => (
                                  <li key={`win-${idx}-${winIdx}`}>
                                    <p>{renderTextWithPostRefs(item.text, {})}</p>
                                    <div className="mt-1 flex flex-wrap gap-3">
                                      {toArray(item.evidenceLinks).map((link, linkIdx) => (
                                        <a
                                          key={`win-link-${idx}-${winIdx}-${linkIdx}`}
                                          href={link}
                                          target="_blank"
                                          rel="noreferrer"
                                          className="text-xs text-zinc-500 hover:text-zinc-300"
                                        >
                                          [source {linkIdx + 1}]
                                        </a>
                                      ))}
                                    </div>
                                  </li>
                                ))}
                              </ul>
                            )}
                          </div>
                        </div>
                      </article>
                    );
                  })}
                </div>
              )}
            </section>
          </>
        ) : null}

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
              {renderTextWithPostRefs(
                analysis.sentiment_summary || 'No summary was returned by analysis.',
                sourcePostsById
              )}
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
          <InsightColumn
            title="Top Themes"
            icon="TH"
            entries={themes}
            sourcePosts={sourcePosts}
            postsById={sourcePostsById}
          />
          <InsightColumn
            title="Pain Points"
            icon="PP"
            entries={painPoints}
            sourcePosts={sourcePosts}
            postsById={sourcePostsById}
            tone="danger"
          />
          <InsightColumn
            title="Community Wins"
            icon="CW"
            entries={wins}
            sourcePosts={sourcePosts}
            postsById={sourcePostsById}
            tone="success"
          />
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

