import React, { useCallback, useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import api from '../api/axios';
import { GameBadge, PrimaryButton, ProgressBar, Tag } from '../components/UI';
import { clampPct, formatNumber, formatShortDate, prettyLabel } from '../lib/community';
import { useAuth } from '../hooks/useAuth';

const statusHintTone = (eligible) => (eligible ? 'text-emerald-700' : 'text-slate-500');

export default function PetitionDetailPage() {
  const { slug } = useParams();
  const { user, login } = useAuth();
  const [petition, setPetition] = useState(null);
  const [supportStatus, setSupportStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [supporting, setSupporting] = useState(false);
  const [shareState, setShareState] = useState('');

  const loadPetition = useCallback(async () => {
    if (!slug) return;
    setLoading(true);
    setError('');
    try {
      const resp = await api.get(`/api/community/petitions/${slug}`);
      setPetition(resp?.data || null);
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : 'Failed to load petition.');
      setPetition(null);
    } finally {
      setLoading(false);
    }
  }, [slug]);

  const loadSupportStatus = useCallback(async () => {
    if (!slug || !user) {
      setSupportStatus(null);
      return;
    }
    try {
      const resp = await api.get(`/api/community/petitions/${slug}/support-status`);
      setSupportStatus(resp?.data || null);
    } catch {
      setSupportStatus(null);
    }
  }, [slug, user]);

  useEffect(() => {
    loadPetition();
  }, [loadPetition]);

  useEffect(() => {
    loadSupportStatus();
  }, [loadSupportStatus]);

  const effectiveSupporters = Number(supportStatus?.supporter_count ?? petition?.supporter_count ?? 0);
  const effectivePct = clampPct(supportStatus?.milestone_progress_pct ?? petition?.milestone_progress_pct ?? 0);
  const nextMilestone = supportStatus?.next_milestone ?? petition?.next_milestone ?? null;
  const currentMilestone = supportStatus?.current_milestone ?? petition?.current_milestone ?? 0;
  const eligibleForStudioPush = Boolean(supportStatus?.eligible_for_studio_push ?? petition?.eligible_for_studio_push);
  const hasSupported = Boolean(supportStatus?.user_has_supported);

  const onToggleSupport = async () => {
    if (!user) {
      await login();
      return;
    }
    if (!slug || supporting) return;

    setSupporting(true);
    setError('');
    try {
      const resp = hasSupported
        ? await api.delete(`/api/community/petitions/${slug}/support`)
        : await api.post(`/api/community/petitions/${slug}/support`);
      const data = resp?.data || null;
      setSupportStatus(data);
      setPetition((prev) => (prev ? {
        ...prev,
        supporter_count: Number(data?.supporter_count || prev.supporter_count || 0),
        current_milestone: Number(data?.current_milestone ?? prev.current_milestone ?? 0),
        next_milestone: data?.next_milestone ?? prev.next_milestone ?? null,
        milestone_progress_pct: Number(data?.milestone_progress_pct ?? prev.milestone_progress_pct ?? 0),
        eligible_for_studio_push: Boolean(data?.eligible_for_studio_push ?? prev.eligible_for_studio_push),
      } : prev));
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : 'Failed to update petition support.');
    } finally {
      setSupporting(false);
    }
  };

  const onShare = async () => {
    const url = window.location.href;
    try {
      if (navigator.share) {
        await navigator.share({ title: petition?.title || 'Sentient Community Petition', url });
        setShareState('Shared');
      } else {
        await navigator.clipboard.writeText(url);
        setShareState('Link copied');
      }
    } catch {
      setShareState('Unable to share');
    }
    setTimeout(() => setShareState(''), 2000);
  };

  if (loading) {
    return <main className="page-shell max-w-5xl"><div className="card-glass p-6 text-slate-500">Loading petition...</div></main>;
  }

  if (!petition) {
    return <main className="page-shell max-w-5xl"><div className="card-glass p-6 text-amber-700">{error || 'Petition not found.'}</div></main>;
  }

  return (
    <main className="page-shell max-w-5xl">
      <section className="card-glass p-6 md:p-8">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div className="flex items-center gap-2 flex-wrap">
            <Tag>{prettyLabel(petition.category)}</Tag>
            <Tag>{prettyLabel(petition.change_type)}</Tag>
            <Tag tone={eligibleForStudioPush ? 'success' : 'warning'}>
              {eligibleForStudioPush ? 'Studio milestone reached' : 'Building support'}
            </Tag>
          </div>
          <button type="button" onClick={onShare} className="btn-ghost px-3 py-2 text-sm font-semibold">Share</button>
        </div>

        <div className="mt-4 flex items-center gap-3">
          <GameBadge name={petition.game_name} logoUrl={petition.game_logo_url} size="lg" />
          <div className="min-w-0">
            <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate-500">Game</p>
            <p className="text-sm md:text-base font-semibold text-slate-800 truncate">{petition.game_name || 'Unknown Game'}</p>
          </div>
        </div>
        <h1 className="hero-title text-3xl md:text-5xl leading-tight mt-3">{petition.title}</h1>
        <p className="mt-4 text-base md:text-lg text-slate-700 leading-relaxed">{petition.summary}</p>

        <div className="mt-6 grid grid-cols-1 md:grid-cols-4 gap-3">
          <div className="stat-tile"><p className="stat-label">Supporters</p><p className="stat-value">{formatNumber(effectiveSupporters)}</p></div>
          <div className="stat-tile"><p className="stat-label">Current milestone</p><p className="stat-value">{formatNumber(currentMilestone || 0)}</p></div>
          <div className="stat-tile"><p className="stat-label">Next milestone</p><p className="stat-value">{nextMilestone ? formatNumber(nextMilestone) : 'Reached'}</p></div>
          <div className="stat-tile"><p className="stat-label">Recent support (7d)</p><p className="stat-value">{formatNumber(petition.recent_supporters_7d || 0)}</p></div>
        </div>

        <div className="mt-5 space-y-2">
          <div className="flex items-center justify-between text-sm"><span className="text-slate-500">Progress to next milestone</span><span className="text-slate-800 font-semibold">{Math.round(effectivePct)}%</span></div>
          <ProgressBar value={effectivePct} />
          <p className={`text-xs ${statusHintTone(eligibleForStudioPush)}`}>
            {eligibleForStudioPush ? 'This petition has crossed the first milestone and can be surfaced to studios.' : `Keep sharing to reach ${nextMilestone ? formatNumber(nextMilestone) : 'the next milestone'}.`}
          </p>
        </div>

        <div className="mt-6 flex items-center gap-3 flex-wrap">
          <PrimaryButton type="button" disabled={supporting} onClick={onToggleSupport}>
            {supporting ? 'Updating…' : hasSupported ? 'Remove Support' : 'Support Petition'}
          </PrimaryButton>
          {!user ? <p className="text-sm text-slate-500">Sign in to support and track petitions.</p> : null}
          {shareState ? <p className="text-sm text-slate-500">{shareState}</p> : null}
        </div>

        {error ? <p className="mt-4 text-sm text-rose-700 bg-rose-50 border border-rose-100 rounded-xl px-3 py-2">{error}</p> : null}
      </section>

      <section className="card-glass p-6 md:p-8 mt-6">
        <div className="flex items-center justify-between gap-3 flex-wrap mb-4">
          <h2 className="panel-title text-2xl md:text-3xl font-bold">Petition details</h2>
          <div className="text-sm text-slate-500">Created {formatShortDate(petition.created_at)} by {petition.created_by_name || 'Community User'}</div>
        </div>
        <div className="whitespace-pre-wrap text-slate-700 leading-relaxed">{petition.body}</div>
      </section>
    </main>
  );
}
