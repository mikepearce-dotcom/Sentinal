const STORAGE_PREFIX = 'sentient_tracker_avg_duration_ms:';
const SMOOTHING = 0.35;
const MIN_DURATION_MS = 250;
const MAX_DURATION_MS = 20 * 60 * 1000;

const canUseStorage = () => {
  return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';
};

const clampDuration = (value) => {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) return 0;
  return Math.min(MAX_DURATION_MS, Math.max(MIN_DURATION_MS, numeric));
};

export const readAverageDurationMs = (key) => {
  if (!key || !canUseStorage()) return 0;

  try {
    const raw = window.localStorage.getItem(`${STORAGE_PREFIX}${key}`);
    if (!raw) return 0;

    const parsed = Number(raw);
    if (!Number.isFinite(parsed) || parsed <= 0) return 0;
    return parsed;
  } catch (_err) {
    return 0;
  }
};

export const updateAverageDurationMs = (key, durationMs) => {
  if (!key) return 0;

  const clamped = clampDuration(durationMs);
  if (clamped <= 0) return readAverageDurationMs(key);

  const previous = readAverageDurationMs(key);
  const next = previous > 0
    ? (previous * (1 - SMOOTHING)) + (clamped * SMOOTHING)
    : clamped;

  if (canUseStorage()) {
    try {
      window.localStorage.setItem(`${STORAGE_PREFIX}${key}`, String(Math.round(next)));
    } catch (_err) {
      return next;
    }
  }

  return next;
};
