import { fetchCoverageHour, fetchHistoryHour, type CoverageCell, type HistoryHour } from './api';

const HOUR_MS = 3_600_000;
const MAX_HOURS = 8;

export const hourOf = (ms: number) => Math.floor(ms / HOUR_MS) * HOUR_MS;

/**
 * In-memory cache of hourly responses (history or coverage): the hour under the slider and its
 * neighbours are held, older ones dropped. A request for an hour already asked for shares one fetch.
 * The current hour is re-fetched after 30 s since it is still growing; a failed fetch is forgotten.
 */
class HourCache<T> {
  private entries = new Map<number, { promise: Promise<T>; at: number }>();

  constructor(private fetcher: (iso: string) => Promise<T>, private maxAgeCurrentMs = 30_000) {}

  get(hourMs: number): Promise<T> {
    const now = Date.now();
    const hit = this.entries.get(hourMs);
    const current = hourMs === hourOf(now);
    if (hit && !(current && now - hit.at > this.maxAgeCurrentMs)) {
      // refresh recency
      this.entries.delete(hourMs);
      this.entries.set(hourMs, hit);
      return hit.promise;
    }
    const promise = this.fetcher(new Date(hourMs).toISOString());
    promise.catch(() => {
      if (this.entries.get(hourMs)?.promise === promise) this.entries.delete(hourMs);
    });
    this.entries.delete(hourMs);
    this.entries.set(hourMs, { promise, at: now });
    while (this.entries.size > MAX_HOURS) {
      this.entries.delete(this.entries.keys().next().value as number);
    }
    return promise;
  }
}

export const historyCache = new HourCache<HistoryHour>((iso) => fetchHistoryHour(iso));
export const coverageCache = new HourCache<CoverageCell[]>((iso) => fetchCoverageHour(iso));
