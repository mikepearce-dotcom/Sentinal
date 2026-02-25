import React from 'react';
import { Link } from 'react-router-dom';
import { clampPct, formatNumber, formatShortDate, prettyLabel, toObject } from '../lib/community';
import { GameBadge, ProgressBar, Tag } from './UI';

export default function PetitionCard({ petition }) {
  const item = toObject(petition);
  const milestonePct = clampPct(item.milestone_progress_pct);

  return (
    <article className="card-glass p-5 card-hover h-full flex flex-col">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex items-start gap-3">
          <GameBadge name={item.game_name} logoUrl={item.game_logo_url} size="md" className="mt-0.5" />
          <div className="min-w-0">
            <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-slate-500 truncate">{item.game_name || 'Unknown Game'}</p>
            <Link to={`/petitions/${item.slug || item.id}`} className="block mt-2">
              <h3 className="font-heading text-2xl font-bold text-slate-900 leading-tight hover:text-sky-700 transition-colors">
                {item.title || 'Untitled petition'}
              </h3>
            </Link>
          </div>
        </div>
        <Tag tone={item.eligible_for_studio_push ? 'success' : 'neutral'}>
          {item.eligible_for_studio_push ? 'Milestone hit' : 'Growing'}
        </Tag>
      </div>

      <p className="mt-3 text-sm text-slate-600 leading-relaxed line-clamp-3">{item.summary || 'No summary provided.'}</p>

      <div className="mt-4 flex items-center gap-2 flex-wrap">
        <Tag>{prettyLabel(item.category)}</Tag>
        <Tag>{prettyLabel(item.change_type)}</Tag>
        <Tag tone="warning">{formatNumber(item.supporter_count)} supporters</Tag>
      </div>

      <div className="mt-4 space-y-2">
        <div className="flex items-center justify-between text-xs text-slate-500">
          <span>Progress to next milestone</span>
          <span>
            {item.next_milestone
              ? `${formatNumber(item.supporter_count)} / ${formatNumber(item.next_milestone)}`
              : `${formatNumber(item.supporter_count)}+`}
          </span>
        </div>
        <ProgressBar value={milestonePct} />
      </div>

      <div className="mt-auto pt-4 flex items-center justify-between gap-3 text-xs text-slate-500">
        <span>Created {formatShortDate(item.created_at)}</span>
        <Link to={`/petitions/${item.slug || item.id}`} className="copy-link">View petition</Link>
      </div>
    </article>
  );
}
