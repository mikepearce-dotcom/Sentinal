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
      className={`px-4 py-2 border border-white/15 text-zinc-300 hover:text-white hover:border-white/30 transition-colors disabled:opacity-50 disabled:cursor-not-allowed ${className}`}
    >
      {children}
    </button>
  );
}

export function Tag({ children, tone = 'neutral' }) {
  const toneClass =
    tone === 'success'
      ? 'text-[#7CFF9A] border-[#7CFF9A]/20 bg-[#7CFF9A]/5'
      : tone === 'warning'
      ? 'text-[#FCEE0A] border-[#FCEE0A]/20 bg-[#FCEE0A]/5'
      : tone === 'danger'
      ? 'text-[#FF4569] border-[#FF4569]/20 bg-[#FF4569]/5'
      : 'text-zinc-300 border-white/10 bg-white/[0.03]';

  return <span className={`inline-flex px-2 py-1 text-xs border ${toneClass}`}>{children}</span>;
}

export function ProgressBar({ value = 0 }) {
  const pct = Math.max(0, Math.min(100, Number(value || 0) || 0));
  return (
    <div className="w-full h-2 bg-black/40 border border-white/10 overflow-hidden">
      <div className="h-full bg-gradient-to-r from-[#00E5FF] to-[#D3F34B]" style={{ width: `${pct}%` }} />
    </div>
  );
}
