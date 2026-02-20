import React, { useEffect, useState } from 'react';

const STAGE_ADVANCE_MS = 3500;

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

const getActiveStageIndex = (stageCount, elapsedMs, estimateMs) => {
  if (!stageCount) return -1;

  if (estimateMs > 0) {
    const progress = Math.min(0.999, elapsedMs / estimateMs);
    return Math.min(stageCount - 1, Math.max(0, Math.floor(progress * stageCount)));
  }

  const index = Math.floor(elapsedMs / STAGE_ADVANCE_MS);
  return Math.min(stageCount - 1, Math.max(0, index));
};

const ActionProgressLoader = ({
  label = 'Working...',
  subtitle = '',
  startedAtMs = 0,
  averageDurationMs = 0,
  compact = false,
  stages = [],
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

  const safeStages = Array.isArray(stages)
    ? stages.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  const activeStageIndex = getActiveStageIndex(safeStages.length, elapsedMs, estimateMs);
  const activeStageNumber = activeStageIndex >= 0 ? activeStageIndex + 1 : 0;

  const etaLabel = estimateMs > 0
    ? (remainingMs > 0 ? `ETA ${formatDuration(remainingMs)}` : 'Wrapping up...')
    : 'ETA calibrating...';

  const visibleStages = (() => {
    if (!safeStages.length) return [];
    if (!compact) return safeStages.map((text, idx) => ({ text, idx }));

    const from = Math.max(0, activeStageIndex - 1);
    const to = Math.min(safeStages.length, activeStageIndex + 2);
    return safeStages.slice(from, to).map((text, offset) => ({ text, idx: from + offset }));
  })();

  return (
    <div className={`action-progress ${compact ? 'action-progress-compact' : ''}`} role="status" aria-live="polite">
      <div className="action-loader" aria-hidden="true" />
      <div className="action-progress-copy">
        <p className="action-progress-label">{label}</p>
        {subtitle ? <p className="action-progress-subtitle">{subtitle}</p> : null}

        {safeStages.length > 0 ? (
          <div className="action-progress-stage-stack">
            <p className="action-progress-pipeline font-mono">
              Pipeline {activeStageNumber}/{safeStages.length}
            </p>
            <ul className="action-progress-stages">
              {visibleStages.map((stage) => {
                const state = stage.idx < activeStageIndex
                  ? 'done'
                  : stage.idx === activeStageIndex
                  ? 'active'
                  : 'pending';

                return (
                  <li
                    key={`stage-${stage.idx}-${stage.text}`}
                    className={`action-progress-stage action-progress-stage-${state}`}
                  >
                    <span className="action-progress-stage-marker" aria-hidden="true" />
                    <span className="action-progress-stage-text">{stage.text}</span>
                  </li>
                );
              })}
            </ul>
          </div>
        ) : null}

        <div className="action-progress-metrics font-mono">
          <span>Elapsed {formatDuration(elapsedMs)}</span>
          <span>{etaLabel}</span>
        </div>
      </div>
    </div>
  );
};

export default ActionProgressLoader;
