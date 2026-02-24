export const toArray = (value) => (Array.isArray(value) ? value : []);
export const toObject = (value) => (value && typeof value === 'object' && !Array.isArray(value) ? value : {});

export const prettyLabel = (value) =>
  String(value || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (m) => m.toUpperCase());

export const clampPct = (value) => {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, n));
};

export const formatShortDate = (value) => {
  if (!value) return 'Unknown';
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return 'Unknown';
  return dt.toLocaleDateString();
};

export const formatNumber = (value) => new Intl.NumberFormat().format(Number(value || 0) || 0);
