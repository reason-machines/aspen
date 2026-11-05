/** Pure helpers for painting and display. No DOM access. */

/** Every grid cell on the segment from (x0, y0) to (x1, y1), inclusive (Bresenham). */
export function lineCells(x0: number, y0: number, x1: number, y1: number): [number, number][] {
  const cells: [number, number][] = [];
  const dx = Math.abs(x1 - x0);
  const dy = -Math.abs(y1 - y0);
  const sx = x0 < x1 ? 1 : -1;
  const sy = y0 < y1 ? 1 : -1;
  let err = dx + dy;
  let x = x0;
  let y = y0;
  for (;;) {
    cells.push([x, y]);
    if (x === x1 && y === y1) return cells;
    const e2 = 2 * err;
    if (e2 >= dy) {
      err += dy;
      x += sx;
    }
    if (e2 <= dx) {
      err += dx;
      y += sy;
    }
  }
}

/** The tile under a point in an element of `rectWidth` x `rectHeight` CSS pixels. */
export function cellAt(
  px: number,
  py: number,
  rectWidth: number,
  rectHeight: number,
  cols: number,
  rows: number,
): { x: number; y: number } | null {
  if (px < 0 || py < 0 || px >= rectWidth || py >= rectHeight) return null;
  return {
    x: Math.min(cols - 1, Math.floor((px / rectWidth) * cols)),
    y: Math.min(rows - 1, Math.floor((py / rectHeight) * rows)),
  };
}

export function shortSha(oid: string): string {
  return oid.slice(0, 7);
}

/** "now", "4 s", "3 min", "2 h" for a duration in milliseconds. */
export function formatDuration(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 1) return 'now';
  if (s < 60) return `${s} s`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m} min`;
  return `${Math.round(m / 60)} h`;
}

/** Shell-quotes a value for the copyable git instructions. */
export function shellQuote(s: string): string {
  return /^[A-Za-z0-9_./:@%+=,-]+$/.test(s) ? s : `'${s.replace(/'/g, `'\\''`)}'`;
}

/** A rate over a sliding window, for "pushes per second". */
export class RateMeter {
  private readonly times: number[] = [];
  private readonly windowMs: number;

  constructor(windowMs: number) {
    this.windowMs = windowMs;
  }

  add(at: number, count = 1): void {
    for (let i = 0; i < count; i++) this.times.push(at);
  }

  perSecond(now: number): number {
    const cutoff = now - this.windowMs;
    let drop = 0;
    while (drop < this.times.length && this.times[drop] <= cutoff) drop++;
    if (drop) this.times.splice(0, drop);
    return (this.times.length * 1000) / this.windowMs;
  }
}
