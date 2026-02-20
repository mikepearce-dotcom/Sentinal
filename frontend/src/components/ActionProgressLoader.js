import React, { useEffect, useState } from 'react';

const formatDuration = (valueMs) => {
  const totalSeconds = Math.max(0, Math.floor(Number(valueMs || 0) / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;

  if (minutes >= 60) {
    const hours = Math.floor(minutes / 60);
    const remainMinutes = minutes % 60;
    return `${hours}:${String(remainMinutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  }

  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
};

const ActionProgressLoader = ({
  label = 'Working...',
  subtitle = '',
  startedAtMs = 0,
  averageDurationMs = 0,
  compact = false,
}) => {
  const [nowMs, setNowMs] = useState(Date.now());

  useEffect(() => {
    if (!startedAtMs) {
      return undefined;
    }

    setNowMs(Date.now());
    const timer = window.setInterval(() => {
      setNowMs(Date.now());
    }, 1000);

    return () => {
      window.clearInterval(timer);
    };
  }, [startedAtMs]);

  const started = Number(startedAtMs || nowMs);
  const elapsedMs = Math.max(0, nowMs - started);
  const estimateMs = Number(averageDurationMs || 0);
  const remainingMs = estimateMs > 0 ? Math.max(0, estimateMs - elapsedMs) : 0;

  const etaLabel = estimateMs > 0
    ? (remainingMs > 0 ? `ETA ${formatDuration(remainingMs)}` : 'Wrapping up...')
    : 'ETA calibrating...';

  return (
    <div className={`action-progress ${compact ? 'action-progress-compact' : ''}`} role="status" aria-live="polite">
      <div className="action-loader" aria-hidden="true" />
      <div className="action-progress-copy">
        <p className="action-progress-label">{label}</p>
        {subtitle ? <p className="action-progress-subtitle">{subtitle}</p> : null}
        <div className="action-progress-metrics font-mono">
          <span>Elapsed {formatDuration(elapsedMs)}</span>
          <span>{etaLabel}</span>
        </div>
      </div>
    </div>
  );
};

export default ActionProgressLoader;
