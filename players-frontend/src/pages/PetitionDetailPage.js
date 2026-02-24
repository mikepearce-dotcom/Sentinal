import React, { useCallback, useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import api from '../api/axios';
import { PrimaryButton, ProgressBar, Tag } from '../components/UI';
import { clampPct, formatNumber, formatShortDate } from '../lib/community';
import { useAuth } from '../hooks/useAuth';

const statusHintTone = (eligible) => (eligible ? 'text-[#7CFF9A]' : 'text-zinc-400');

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
    return <main className="max-w-5xl mx-auto px-4 md:px-8 py-10"><div className="card-glass p-6 text-zinc-400">Loading petition...</div></main>;
  }

  if (!petition) {
    return <main className="max-w-5xl mx-auto px-4 md:px-8 py-10"><div className="card-glass p-6 text-amber-300">{error || 'Petition not found.'}</div></main>;
  }

  return (
    <main className="max-w-5xl mx-auto px-4 md:px-8 py-8">
      <section className="card-glass p-6 md:p-8">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div className="flex items-center gap-2 flex-wrap">
            <Tag>{String(petition.category || '').replace(/_/g, ' ')}</Tag>
            <Tag>{String(petition.change_type || '').replace(/_/g, ' ')}</Tag>
            <Tag tone={eligibleForStudioPush ? 'success' : 'warning'}>
              {eligibleForStudioPush ? 'Studio milestone reached' : 'Building support'}
            </Tag>
          </div>
          <button type="button" onClick={onShare} className="text-sm text-zinc-300 hover:text-white">Share petition</button>
        </div>

        <p className="font-mono text-xs uppercase tracking-[0.2em] text-zinc-500 mt-4">{petition.game_name || 'Unknown Game'}</p>
        <h1 className="font-heading text-4xl md:text-5xl font-black leading-tight mt-3">{petition.title}</h1>
        <p className="mt-4 text-lg text-zinc-200 leading-relaxed">{petition.summary}</p>

        <div className="mt-6 grid grid-cols-1 md:grid-cols-4 gap-3">
          <div className="border border-white/10 bg-black/20 p-3"><p className="text-xs text-zinc-500">Supporters</p><p className="mt-1 font-heading text-3xl font-black">{formatNumber(effectiveSupporters)}</p></div>
          <div className="border border-white/10 bg-black/20 p-3"><p className="text-xs text-zinc-500">Current Milestone</p><p className="mt-1 font-heading text-3xl font-black">{formatNumber(currentMilestone || 0)}</p></div>
          <div className="border border-white/10 bg-black/20 p-3"><p className="text-xs text-zinc-500">Next Milestone</p><p className="mt-1 font-heading text-3xl font-black">{nextMilestone ? formatNumber(nextMilestone) : 'Reached'}</p></div>
          <div className="border border-white/10 bg-black/20 p-3"><p className="text-xs text-zinc-500">Recent Support (7d)</p><p className="mt-1 font-heading text-3xl font-black">{formatNumber(petition.recent_supporters_7d || 0)}</p></div>
        </div>

        <div className="mt-5 space-y-2">
          <div className="flex items-center justify-between text-sm"><span className="text-zinc-400">Progress to next milestone</span><span className="text-zinc-200">{Math.round(effectivePct)}%</span></div>
          <ProgressBar value={effectivePct} />
          <p className={`text-xs ${statusHintTone(eligibleForStudioPush)}`}>
            {eligibleForStudioPush ? 'This petition has crossed the first milestone and can be surfaced to studios.' : `Keep sharing to reach ${nextMilestone ? formatNumber(nextMilestone) : 'the next milestone'}.`}
          </p>
        </div>

        <div className="mt-6 flex items-center gap-3 flex-wrap">
          <PrimaryButton type="button" disabled={supporting} onClick={onToggleSupport}>{supporting ? 'Updating…' : hasSupported ? 'Remove Support' : 'Support Petition'}</PrimaryButton>
          {!user ? <p className="text-sm text-zinc-400">Sign in to support and track petitions.</p> : null}
          {shareState ? <p className="text-sm text-zinc-400">{shareState}</p> : null}
        </div>

        {error ? <p className="mt-4 text-sm text-amber-300">{error}</p> : null}
      </section>

      <section className="card-glass p-6 md:p-8 mt-6">
        <div className="flex items-center justify-between gap-3 flex-wrap mb-4">
          <h2 className="font-heading text-3xl font-black">Petition Details</h2>
          <div className="text-sm text-zinc-400">Created {formatShortDate(petition.created_at)} by {petition.created_by_name || 'Community User'}</div>
        </div>
        <div className="whitespace-pre-wrap text-zinc-200 leading-relaxed">{petition.body}</div>
      </section>
    </main>
  );
}
