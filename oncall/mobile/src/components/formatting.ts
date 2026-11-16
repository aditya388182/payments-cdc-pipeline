export function fmtInt(n: number): string {
  return Math.round(n).toLocaleString("en-US");
}

export function fmtRate(n: number): string {
  return n >= 100 ? String(Math.round(n)) : (Math.round(n * 10) / 10).toFixed(1);
}

export function fmtSeconds(s: number): string {
  if (s < 1) return "0s";
  if (s < 90) return `${String(Math.round(s))}s`;
  if (s < 5400) return `${String(Math.round(s / 60))}m`;
  return `${String(Math.round(s / 3600))}h`;
}

export function fmtMoney(minor: number, currency: string): string {
  return `${(minor / 100).toFixed(2)} ${currency}`;
}

export function fmtTime(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().replace("T", " ").slice(0, 19) + "Z";
}

export function shortId(id: string): string {
  return id.length > 13 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id;
}
