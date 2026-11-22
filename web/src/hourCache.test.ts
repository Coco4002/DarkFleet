import { afterEach, describe, expect, it, vi } from 'vitest';
import { HourCache } from './hourCache';

const HOUR = Date.parse('2026-11-20T11:00:00Z');

describe('HourCache', () => {
  afterEach(() => vi.useRealTimers());

  it('shares a fetch for a minute, then fetches the hour again (late data can still arrive)', async () => {
    vi.useFakeTimers();
    const fetcher = vi.fn(async (iso: string) => iso);
    const cache = new HourCache<string>(fetcher);

    vi.setSystemTime(Date.parse('2026-11-20T12:30:00Z'));
    await cache.get(HOUR);
    vi.setSystemTime(Date.parse('2026-11-20T12:30:45Z'));
    await cache.get(HOUR);
    expect(fetcher).toHaveBeenCalledTimes(1);

    vi.setSystemTime(Date.parse('2026-11-20T12:31:30Z'));
    await cache.get(HOUR);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });
});
