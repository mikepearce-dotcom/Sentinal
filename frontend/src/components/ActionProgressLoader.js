import React, { useEffect, useMemo, useState } from 'react';

const STAGE_ADVANCE_MS = 3500;

const LOADER_PHASES = {
  idle: {
    label: 'BOOT',
    motion: {
      sweepDuration: '3.2s',
      coreDuration: '1.8s',
      orbDuration: '2.4s',
      ringADuration: '2.9s',
      ringBDuration: '3.7s',
      ringCDuration: '5.3s',
    },
  },
  acquire: {
    label: 'DISCOVERY',
    motion: {
      sweepDuration: '2.8s',
      coreDuration: '1.5s',
      orbDuration: '2.1s',
      ringADuration: '2.5s',
      ringBDuration: '3.3s',
      ringCDuration: '4.8s',
    },
  },
  compute: {
    label: 'ANALYSIS',
    motion: {
      sweepDuration: '2.4s',
      coreDuration: '1.2s',
      orbDuration: '1.7s',
      ringADuration: '2.2s',
      ringBDuration: '3.0s',
      ringCDuration: '4.4s',
    },
  },
  synthesize: {
    label: 'WEIGHTING',
    motion: {
      sweepDuration: '2.0s',
      coreDuration: '1.0s',
      orbDuration: '1.4s',
      ringADuration: '1.9s',
      ringBDuration: '2.7s',
      ringCDuration: '4.0s',
    },
  },
  finalize: {
    label: 'COMPILE',
    motion: {
      sweepDuration: '2.9s',
      coreDuration: '0.95s',
      orbDuration: '1.2s',
      ringADuration: '2.4s',
      ringBDuration: '3.2s',
      ringCDuration: '4.6s',
    },
  },
};

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

const getLoaderPhase = (activeStageIndex, stageCount) => {
  if (stageCount <= 0 || activeStageIndex < 0) {
    return 'idle';
  }

  const progress = (activeStageIndex + 1) / stageCount;
  if (progress < 0.34) return 'acquire';
  if (progress < 0.67) return 'compute';
  if (progress < 0.95) return 'synthesize';
  return 'finalize';
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
  const loaderPhase = getLoaderPhase(activeStageIndex, safeStages.length);

  const loaderPhaseConfig = LOADER_PHASES[loaderPhase] || LOADER_PHASES.idle;
  const loaderStyle = useMemo(
    () => ({
      '--loader-sweep-duration': loaderPhaseConfig.motion.sweepDuration,
      '--loader-core-duration': loaderPhaseConfig.motion.coreDuration,
      '--loader-orb-duration': loaderPhaseConfig.motion.orbDuration,
      '--loader-ring-a-duration': loaderPhaseConfig.motion.ringADuration,
      '--loader-ring-b-duration': loaderPhaseConfig.motion.ringBDuration,
      '--loader-ring-c-duration': loaderPhaseConfig.motion.ringCDuration,
    }),
    [loaderPhaseConfig]
  );

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
      <div
        className={`action-loader ${compact ? 'action-loader-compact' : ''} action-loader-phase-${loaderPhase}`}
        style={loaderStyle}
        aria-hidden="true"
      >
        <span className="action-loader-ring action-loader-ring-a" />
        <span className="action-loader-ring action-loader-ring-b" />
        <span className="action-loader-ring action-loader-ring-c" />
        <span className="action-loader-beam" />
        <span className="action-loader-orb action-loader-orb-a" />
        <span className="action-loader-orb action-loader-orb-b" />
        <span className="action-loader-orb action-loader-orb-c" />
        <span className="action-loader-core" />
      </div>
      <div className="action-progress-copy">
        <p className="action-progress-label">{label}</p>
        {subtitle ? <p className="action-progress-subtitle">{subtitle}</p> : null}

        {safeStages.length > 0 ? (
          <div className="action-progress-stage-stack">
            <p className="action-progress-pipeline font-mono">
              Pipeline {activeStageNumber}/{safeStages.length} · {loaderPhaseConfig.label}
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
