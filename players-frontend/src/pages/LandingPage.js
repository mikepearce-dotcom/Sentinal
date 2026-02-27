import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import api from '../api/axios';
import PetitionCard from '../components/PetitionCard';
import { formatNumber, toArray } from '../lib/community';
import { useAuth } from '../hooks/useAuth';

const HERO_CARD_LIMIT = 6;
const HERO_COLLAGE_LIMIT = 5;

function upscaleIgdbHeroImage(url) {
  const raw = String(url || '').trim();
  if (!raw) return '';
  if (!raw.includes('images.igdb.com')) return raw;
  return raw
    .replace('/t_cover_small/', '/t_cover_big/')
    .replace('/t_thumb/', '/t_cover_big/')
    .replace('/t_micro/', '/t_cover_big/');
}

export default function LandingPage() {
  const { user } = useAuth();
  const [featured, setFeatured] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;

    api.get('/api/community/petitions', { params: { sort: 'momentum', limit: 12, page: 1 } })
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

  const heroPreviewSource = toArray(featured)[0] || null;
  const heroPreviewTitleRaw = String(heroPreviewSource?.title || 'Add More Customisation Options').trim();
  const heroPreviewTitle = heroPreviewTitleRaw.length > 54
    ? `${heroPreviewTitleRaw.slice(0, 54).trimEnd()}...`
    : heroPreviewTitleRaw;
  const heroPreviewSupporters = Number(heroPreviewSource?.supporter_count || 2184) || 2184;
  const heroPreviewGoal = Number(heroPreviewSource?.next_milestone || Math.max(heroPreviewSupporters + 316, 2500)) || 2500;
  const featuredCards = toArray(featured).slice(0, HERO_CARD_LIMIT);

  const heroCollageItems = [];
  const heroCollageSeen = new Set();
  const featuredItems = toArray(featured);

  for (const item of featuredItems) {
    const baseLogoUrl = String(item?.game_logo_url || '').trim();
    const logoUrl = upscaleIgdbHeroImage(baseLogoUrl);
    const gameName = String(item?.game_name || '').trim() || 'Game';
    const dedupeKey = `${gameName.toLowerCase()}|${baseLogoUrl}`;
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
    for (const item of featuredItems) {
      const baseLogoUrl = String(item?.game_logo_url || '').trim();
      const logoUrl = upscaleIgdbHeroImage(baseLogoUrl);
      const gameName = String(item?.game_name || '').trim() || 'Game';
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

  return (
    <main>
      <section className="page-shell pt-8 md:pt-12">
        <div className="hero-grid">
          <div className="hero-panel hero-panel-main p-6 md:p-8 xl:p-10">
            <p className="section-eyebrow">Backed by players</p>
            <h1 className="hero-title text-4xl md:text-6xl leading-[0.95] mt-4 max-w-3xl">
              Make game changes happen.
            </h1>
            <p className="hero-subtitle mt-5 text-base md:text-lg leading-relaxed max-w-2xl">
              Start a petition, rally players behind one clear idea, and build real momentum.
              When enough players back the same request, it rises to the top.
            </p>

            <div className="mt-7 flex flex-wrap gap-3 hero-cta-row">
              <Link to={user ? '/petitions/new' : '/signup'} className="btn-primary hero-cta-primary px-5 py-3 text-sm md:text-base">
                <span>Start a Petition</span>
              </Link>
              <Link to="/petitions" className="btn-secondary px-5 py-3 text-sm md:text-base font-semibold">
                Explore Player Requests
              </Link>
            </div>

            <div className="mt-8 grid grid-cols-1 sm:grid-cols-3 gap-3">
              <div className="feature-card">
                <h3>Clear ideas</h3>
                <p>One focused change per petition so players know exactly what they are backing.</p>
              </div>
              <div className="feature-card">
                <h3>Real momentum</h3>
                <p>Track supporters and hit visible goals together.</p>
              </div>
              <div className="feature-card">
                <h3>Rising requests</h3>
                <p>The most backed petitions climb higher and get more visibility.</p>
              </div>
            </div>
          </div>

          <div className="hero-panel hero-panel-visual p-4 md:p-5">
            <div className="hero-visual hero-petition-preview-wrap">
              <div className="hero-collage" aria-hidden="true">
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

              <div className="hero-pulse-orb hero-pulse-orb-a" />
              <div className="hero-pulse-orb hero-pulse-orb-b" />
              <div className="hero-pulse-orb hero-pulse-orb-c" />

              <div className="hero-petition-chip hero-petition-chip-a">Trending idea</div>
              <div className="hero-petition-chip hero-petition-chip-b">Backed by players</div>

              <div className="hero-petition-preview card-glass">
                <p className="hero-petition-label">Petition Title</p>
                <h3 className="hero-petition-title">{heroPreviewTitle}</h3>

                <div className="hero-petition-supporters-row">
                  <p className="hero-petition-supporters">{formatNumber(heroPreviewSupporters)} supporters</p>
                  <span className="hero-petition-status">Rising</span>
                </div>

                <div className="hero-petition-progress-wrap" aria-hidden="true">
                  <div className="hero-petition-progress-fill" />
                </div>
                <p className="hero-petition-goal">Next goal: {formatNumber(heroPreviewGoal)} supporters</p>

                <button type="button" className="hero-petition-cta">Back This Petition</button>
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
