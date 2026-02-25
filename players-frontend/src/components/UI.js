import React from 'react';

export function PrimaryButton({ className = '', children, ...props }) {
  return (
    <button
      {...props}
      className={`btn-primary px-5 py-3 text-sm disabled:opacity-60 disabled:cursor-not-allowed ${className}`}
    >
      <span>{children}</span>
    </button>
  );
}

export function GhostButton({ className = '', children, ...props }) {
  return (
    <button
      {...props}
      className={`btn-ghost px-4 py-2 text-sm font-semibold disabled:opacity-50 disabled:cursor-not-allowed ${className}`}
    >
      {children}
    </button>
  );
}

export function Tag({ children, tone = 'neutral' }) {
  const toneClass =
    tone === 'success'
      ? 'text-emerald-700 border-emerald-200 bg-emerald-50'
      : tone === 'warning'
      ? 'text-amber-700 border-amber-200 bg-amber-50'
      : tone === 'danger'
      ? 'text-rose-700 border-rose-200 bg-rose-50'
      : 'text-slate-700 border-slate-200 bg-white/80';

  return (
    <span className={`inline-flex items-center px-2.5 py-1 text-xs font-semibold rounded-full border ${toneClass}`}>
      {children}
    </span>
  );
}

export function ProgressBar({ value = 0 }) {
  const pct = Math.max(0, Math.min(100, Number(value || 0) || 0));
  return (
    <div className="progress-track" aria-hidden="true">
      <div className="progress-fill" style={{ width: `${pct}%` }} />
    </div>
  );
}

function gameInitials(name) {
  const words = String(name || '').trim().split(/\s+/).filter(Boolean);
  if (!words.length) return 'GM';
  if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
  return `${words[0][0] || ''}${words[1][0] || ''}`.toUpperCase();
}

export function GameBadge({ name = '', logoUrl = '', size = 'md', className = '' }) {
  const [imgFailed, setImgFailed] = React.useState(false);
  const showImage = Boolean(logoUrl) && !imgFailed;

  const sizeClass =
    size === 'sm'
      ? 'h-9 w-9 rounded-lg text-[10px]'
      : size === 'lg'
      ? 'h-14 w-14 rounded-xl text-sm'
      : 'h-11 w-11 rounded-xl text-xs';

  return (
    <span
      className={`relative inline-flex shrink-0 items-center justify-center overflow-hidden border border-slate-200 bg-gradient-to-br from-white to-slate-100 text-slate-700 shadow-sm ${sizeClass} ${className}`.trim()}
      aria-hidden="true"
    >
      {showImage ? (
        <img
          src={logoUrl}
          alt=""
          className="h-full w-full object-cover"
          loading="lazy"
          onError={() => setImgFailed(true)}
        />
      ) : null}
      {!showImage ? (
        <span className="font-heading font-bold tracking-tight text-slate-700">{gameInitials(name)}</span>
      ) : null}
    </span>
  );
}
