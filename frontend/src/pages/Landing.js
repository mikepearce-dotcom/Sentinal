import React, { useContext } from 'react';
import { Link } from 'react-router-dom';
import { AuthContext } from '../context/AuthContext';

const FEATURE_CARDS = [
  {
    title: 'Player Signal Engine',
    text: 'Continuously scans Reddit communities, scores post quality, and surfaces the feedback that actually matters to product decisions.',
  },
  {
    title: 'AI Sentiment Intelligence',
    text: 'Turns noisy discussion into structured sentiment, themes, pain points, and wins so your team can move from gut feeling to evidence.',
  },
  {
    title: 'Cross-Community View',
    text: 'Run combined scans across multiple subreddits to compare sentiment patterns and identify where advocacy or friction is concentrated.',
  },
];

const WORKFLOW = [
  {
    title: 'Track Your Game',
    text: 'Add a game once. Smart subreddit discovery gives you relevant communities in seconds.',
  },
  {
    title: 'Run Automated Scans',
    text: 'Collect high-signal posts and comments with ranking, diversity, and recency weighting built in.',
  },
  {
    title: 'Ship With Confidence',
    text: 'Use structured output to prioritize fixes, messaging, live-ops beats, and roadmap bets.',
  },
];

const KPI_ITEMS = [
  { label: 'Faster Insight Cycles', value: '10x' },
  { label: 'Communities Per Scan', value: '5' },
  { label: 'Player Signals Processed', value: '24/7' },
];

const Landing = () => {
  const { user } = useContext(AuthContext);

  return (
    <div className="landing-page min-h-screen bg-[#07080c] text-white">
      <div className="landing-grid-overlay" aria-hidden="true" />

      <header className="sticky top-0 z-40 border-b border-white/10 landing-header-backdrop">
        <div className="max-w-6xl mx-auto px-6 md:px-10 py-4 flex items-center justify-between gap-4">
          <Link to="/" className="flex items-center gap-3">
            <span className="w-9 h-9 rounded-sm bg-[#D3F34B] text-black font-heading font-black grid place-items-center">
              S
            </span>
            <div>
              <p className="font-heading font-black tracking-wide text-sm md:text-base">SENTIENT TRACKER</p>
              <p className="font-mono text-[10px] md:text-xs text-zinc-400 tracking-[0.2em] uppercase">Player Intel Platform</p>
            </div>
          </Link>

          <nav className="hidden md:flex items-center gap-6 text-sm text-zinc-300">
            <a href="#features" className="hover:text-white transition-colors">Features</a>
            <a href="#workflow" className="hover:text-white transition-colors">Workflow</a>
            <a href="#pricing" className="hover:text-white transition-colors">Get Started</a>
          </nav>

          <div className="flex items-center gap-2 md:gap-3">
            {user ? (
              <Link
                to="/app"
                className="px-4 py-2 border border-[#00E5FF]/35 text-[#8BE8FF] hover:text-white hover:border-[#00E5FF]/70 text-sm"
              >
                Open Dashboard
              </Link>
            ) : (
              <>
                <Link
                  to="/login"
                  className="px-4 py-2 border border-white/20 text-zinc-200 hover:text-white hover:border-white/40 text-sm"
                >
                  Log In
                </Link>
                <Link
                  to="/signup"
                  className="px-4 py-2 bg-[#D3F34B]/20 border border-[#D3F34B]/45 text-[#eef7bf] hover:text-white hover:border-[#D3F34B]/70 text-sm"
                >
                  Sign Up
                </Link>
              </>
            )}
          </div>
        </div>
      </header>

      <main className="relative z-10">
        <section className="border-b border-white/10">
          <div className="max-w-6xl mx-auto px-6 md:px-10 py-14 md:py-24 grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
            <div>
              <p className="inline-flex items-center gap-2 px-3 py-1 border border-[#00E5FF]/30 bg-[#00E5FF]/10 text-[#8BE8FF] font-mono text-xs tracking-[0.16em] uppercase">
                Revenue-Leaning Player Insights
              </p>

              <h1 className="font-heading text-4xl md:text-6xl font-black leading-[0.95] mt-6">
                Stop Guessing What Players Feel.
                <span className="block text-[#D3F34B]">Start Shipping What They Want.</span>
              </h1>

              <p className="mt-6 text-zinc-300 text-lg leading-relaxed max-w-xl">
                Sentient Tracker transforms raw Reddit chatter into decision-ready sentiment intelligence so studios,
                product teams, and community leads can prioritize faster and scale confidence.
              </p>

              <div className="mt-8 flex flex-wrap items-center gap-3">
                <Link to={user ? '/app' : '/signup'} className="btn-primary px-6 py-3">
                  <span>{user ? 'Open Platform' : 'Start Free'}</span>
                </Link>
                <Link
                  to={user ? '/app' : '/login'}
                  className="px-6 py-3 border border-white/20 text-zinc-200 hover:text-white hover:border-white/40"
                >
                  See Product Flow
                </Link>
              </div>

              <div className="mt-10 grid grid-cols-1 sm:grid-cols-3 gap-3">
                {KPI_ITEMS.map((item) => (
                  <article key={item.label} className="card-glass p-4 border border-white/10">
                    <p className="font-heading text-3xl font-black text-[#D3F34B] leading-none">{item.value}</p>
                    <p className="mt-2 text-xs text-zinc-400 uppercase tracking-[0.12em]">{item.label}</p>
                  </article>
                ))}
              </div>
            </div>

            <div className="landing-hero-shell">
              <div className="landing-hero-visual" aria-hidden="true">
                <div className="landing-core" />
                <div className="landing-scan-beam" />

                <div className="landing-orbit landing-orbit-a" />
                <div className="landing-orbit landing-orbit-b" />
                <div className="landing-orbit landing-orbit-c" />

                <article className="landing-signal-card landing-signal-card-a">
                  <p className="font-mono text-[10px] tracking-[0.16em] uppercase text-[#8BE8FF]">Signal</p>
                  <p className="text-sm text-white mt-1">Matchmaking frustration trend</p>
                </article>

                <article className="landing-signal-card landing-signal-card-b">
                  <p className="font-mono text-[10px] tracking-[0.16em] uppercase text-[#7CFF9A]">Win</p>
                  <p className="text-sm text-white mt-1">Retention driver detected</p>
                </article>

                <article className="landing-signal-card landing-signal-card-c">
                  <p className="font-mono text-[10px] tracking-[0.16em] uppercase text-[#FCEE0A]">Theme</p>
                  <p className="text-sm text-white mt-1">Progression pacing discussion</p>
                </article>
              </div>
            </div>
          </div>
        </section>

        <section id="features" className="max-w-6xl mx-auto px-6 md:px-10 py-16 md:py-20">
          <div className="max-w-3xl">
            <p className="font-mono text-xs tracking-[0.18em] text-zinc-400 uppercase">Why Teams Switch</p>
            <h2 className="font-heading text-3xl md:text-5xl font-black mt-3 leading-tight">
              Built to convert community noise into product advantage.
            </h2>
          </div>

          <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-4">
            {FEATURE_CARDS.map((item) => (
              <article key={item.title} className="card-glass p-6 border border-white/10">
                <h3 className="font-heading text-2xl font-bold leading-tight">{item.title}</h3>
                <p className="mt-3 text-zinc-300 leading-relaxed">{item.text}</p>
              </article>
            ))}
          </div>
        </section>

        <section id="workflow" className="border-y border-white/10 bg-black/20">
          <div className="max-w-6xl mx-auto px-6 md:px-10 py-16 md:py-20">
            <div className="max-w-3xl">
              <p className="font-mono text-xs tracking-[0.18em] text-zinc-400 uppercase">How It Works</p>
              <h2 className="font-heading text-3xl md:text-5xl font-black mt-3 leading-tight">
                From raw threads to roadmap-ready decisions in minutes.
              </h2>
            </div>

            <div className="mt-10 grid grid-cols-1 md:grid-cols-3 gap-4">
              {WORKFLOW.map((step, index) => (
                <article key={step.title} className="card-glass p-6 border border-white/10">
                  <p className="font-mono text-xs tracking-[0.16em] text-[#00E5FF] uppercase">Step {index + 1}</p>
                  <h3 className="font-heading text-2xl font-bold mt-2">{step.title}</h3>
                  <p className="mt-3 text-zinc-300 leading-relaxed">{step.text}</p>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section id="pricing" className="max-w-6xl mx-auto px-6 md:px-10 py-16 md:py-20">
          <div className="card-glass p-8 md:p-12 border border-[#D3F34B]/25 relative overflow-hidden">
            <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(211,243,75,0.2),transparent_55%)]" aria-hidden="true" />
            <div className="relative z-10 max-w-3xl">
              <p className="font-mono text-xs tracking-[0.18em] text-[#dff38f] uppercase">Start Capturing Signal</p>
              <h2 className="font-heading text-3xl md:text-5xl font-black mt-3 leading-tight">
                Ready to turn player feedback into growth?
              </h2>
              <p className="mt-4 text-zinc-300 text-lg">
                Join teams using Sentient Tracker to reduce analysis lag, sharpen roadmap choices, and improve player trust.
              </p>
              <div className="mt-8 flex flex-wrap gap-3">
                <Link to={user ? '/app' : '/signup'} className="btn-primary px-6 py-3">
                  <span>{user ? 'Go To Dashboard' : 'Create Free Account'}</span>
                </Link>
                {!user ? (
                  <Link
                    to="/login"
                    className="px-6 py-3 border border-white/20 text-zinc-200 hover:text-white hover:border-white/40"
                  >
                    I already have an account
                  </Link>
                ) : null}
              </div>
            </div>
          </div>
        </section>
      </main>
    </div>
  );
};

export default Landing;

