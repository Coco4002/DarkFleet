import { afterEach, describe, expect, it, vi } from 'vitest';
import { HourCache } from './hourCache';

const HOUR = Date.parse('2026-11-20T11:00:00Z');

describe('HourCache', () => {
  afterEach(() => vi.useRealTimers());

  it('fetches an hour again if it was fetched before the hour had settled, and keeps it once settled', async () => {
    vi.useFakeTimers();
    const fetcher = vi.fn(async (iso: string) => iso);
    const cache = new HourCache<string>(fetcher);

    vi.setSystemTime(Date.parse('2026-11-20T11:59:45Z')); // just before the hour ends
    await cache.get(HOUR);
    vi.setSystemTime(Date.parse('2026-11-20T12:30:00Z')); // over, and settled
    await cache.get(HOUR);
    expect(fetcher).toHaveBeenCalledTimes(2);

    vi.setSystemTime(Date.parse('2026-11-20T18:00:00Z'));
    await cache.get(HOUR);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });
});
