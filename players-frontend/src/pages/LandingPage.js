import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { CheckCircle2 } from 'lucide-react';
import api from '../api/axios';
import PetitionCard from '../components/PetitionCard';
import { toArray } from '../lib/community';
import { useAuth } from '../hooks/useAuth';

const HERO_CARD_LIMIT = 6;
const HERO_COLLAGE_LIMIT = 5;

function upscaleIgdbHeroImage(url) {
  const raw = String(url || '').trim();
  if (!raw) return '';
  if (!raw.includes('images.igdb.com')) return raw;
  return raw
    .replace('/t_cover_small/', '/t_cover_big_2x/')
    .replace('/t_cover_big/', '/t_cover_big_2x/')
    .replace('/t_thumb/', '/t_cover_big_2x/')
    .replace('/t_micro/', '/t_cover_big_2x/');
}

export default function LandingPage() {
  const { user } = useAuth();
  const [featured, setFeatured] = useState([]);
  const [trendingGames, setTrendingGames] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;

    Promise.all([
      api.get('/api/community/petitions', { params: { sort: 'momentum', limit: 12, page: 1 } }),
      api.get('/api/community/games/trending', { params: { limit: 5 } }),
    ])
      .then(([petitionsResp, trendingResp]) => {
        if (cancelled) return;
        setFeatured(toArray(petitionsResp?.data?.items));
        setTrendingGames(toArray(trendingResp?.data));
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

  const featuredCards = toArray(featured).slice(0, HERO_CARD_LIMIT);
  const heroUsps = [
    {
      title: 'Clear ideas',
      copy: 'One focused change per petition so players know what they are backing.',
    },
    {
      title: 'Real momentum',
      copy: 'Track supporters, hit visible goals, and share progress.',
    },
    {
      title: 'Rising requests',
      copy: 'The most backed ideas climb higher and get seen faster.',
    },
  ];

  const heroCollageItems = [];
  const heroCollageSeen = new Set();
  const trendingItems = toArray(trendingGames);

  for (const item of trendingItems) {
    const logoUrl = upscaleIgdbHeroImage(String(item?.logo_url || '').trim());
    const gameName = String(item?.name || '').trim() || 'Game';
    const dedupeKey = `${gameName.toLowerCase()}|${logoUrl}`;
    if (!logoUrl || heroCollageSeen.has(dedupeKey)) {
      continue;
    }
    heroCollageSeen.add(dedupeKey);
    heroCollageItems.push({
      id: String(item?.id || item?.slug || dedupeKey),
      gameName,
      logoUrl,
    });
    if (heroCollageItems.length >= HERO_COLLAGE_LIMIT) {
      break;
    }
  }

  if (heroCollageItems.length < HERO_COLLAGE_LIMIT) {
    for (const item of trendingItems) {
      const logoUrl = upscaleIgdbHeroImage(String(item?.logo_url || '').trim());
      const gameName = String(item?.name || '').trim() || 'Game';
      if (!logoUrl) {
        continue;
      }
      heroCollageItems.push({
        id: `${String(item?.id || item?.slug || gameName)}-repeat-${heroCollageItems.length}`,
        gameName,
        logoUrl,
      });
      if (heroCollageItems.length >= HERO_COLLAGE_LIMIT) {
        break;
      }
    }
  }

  while (heroCollageItems.length < HERO_COLLAGE_LIMIT) {
    heroCollageItems.push({
      id: `placeholder-${heroCollageItems.length}`,
      gameName: 'Game',
      logoUrl: '',
    });
  }

  const heroBackdropImage = heroCollageItems.find((item) => item.logoUrl)?.logoUrl || '';

  return (
    <main>
      <section className="community-hero">
        <div className="hero-collage hero-collage-stage" aria-hidden="true">
          <div className={`hero-collage-base ${heroBackdropImage ? '' : 'hero-collage-base-fallback'}`.trim()}>
            {heroBackdropImage ? (
              <img src={heroBackdropImage} alt="" className="hero-collage-base-image" loading="lazy" />
            ) : null}
          </div>

          {heroCollageItems.map((item, index) => (
            <div
              key={item.id}
              className={`hero-collage-slice hero-collage-slice-${index + 1} ${item.logoUrl ? '' : 'hero-collage-slice-fallback'}`.trim()}
            >
              {item.logoUrl ? (
                <img src={item.logoUrl} alt="" className="hero-collage-image" loading="lazy" />
              ) : (
                <span className="hero-collage-fallback-text">{item.gameName}</span>
              )}
            </div>
          ))}
        </div>

        <div className="community-hero-overlay" aria-hidden="true" />

        <div className="page-shell community-hero-shell">
          <div className="community-hero-content">
            <p className="community-hero-eyebrow">BACKED BY PLAYERS</p>
            <h1 className="community-hero-title">Make game changes happen.</h1>
            <p className="community-hero-subtitle">
              Start a petition, rally players behind one clear idea, and build real momentum.
              When enough players back the same request, it rises to the top.
            </p>

            <div className="community-hero-cta-row">
              <Link to={user ? '/petitions/new' : '/signup'} className="community-hero-cta community-hero-cta-primary">
                Start a Petition
              </Link>
              <Link to="/petitions" className="community-hero-cta community-hero-cta-secondary">
                Explore Player Requests
              </Link>
            </div>

            <ul className="community-hero-usp-list" aria-label="Why players use petitions">
              {heroUsps.map((item) => (
                <li key={item.title} className="community-hero-usp-item">
                  <CheckCircle2 className="community-hero-usp-icon" aria-hidden="true" />
                  <p className="community-hero-usp-text">
                    <strong>{item.title}:</strong> {item.copy}
                  </p>
                </li>
              ))}
            </ul>
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
              <p>As support grows, the request gains more visibility and becomes easier for more players to rally behind.</p>
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
        ) : featuredCards.length === 0 ? (
          <div className="card-glass p-6 text-slate-500">No petitions yet. Be the first player to start one.</div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
            {featuredCards.map((item) => <PetitionCard key={item.id || item.slug} petition={item} />)}
          </div>
        )}
      </section>
    </main>
  );
}

