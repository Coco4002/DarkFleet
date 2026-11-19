import { useEffect, useState } from 'react';
import {
  analystEventsUrl,
  listDetections,
  listProposals,
  type Detection,
  type Session,
} from './analystApi';

/**
 * What analysts see on top of the public map: currently open detections and the count of proposals
 * waiting for a decision. Refetched when the server announces a change (server-sent events), and
 * once a minute as a fallback.
 */
export function useAnalystFeed(session: Session | null) {
  const [open, setOpen] = useState<Detection[]>([]);
  const [pending, setPending] = useState(0);
  const [tick, setTick] = useState(0);
  const active = session?.authenticated === true;

  useEffect(() => {
    if (!active) {
      setOpen([]);
      setPending(0);
      return;
    }
    let cancelled = false;
    async function load() {
      try {
        const [d, p] = await Promise.all([listDetections({ status: 'OPEN' }), listProposals('PENDING')]);
        if (cancelled) return;
        setOpen(d.items);
        setPending(p.items.length + (p.hasMore ? 1 : 0));
        setTick((t) => t + 1);
      } catch {
        // session expired or server busy: the next change or minute tries again
      }
    }
    void load();
    const es = new EventSource(analystEventsUrl);
    es.addEventListener('detection', () => void load());
    es.addEventListener('proposal', () => void load());
    const timer = setInterval(() => void load(), 60_000);
    return () => {
      cancelled = true;
      es.close();
      clearInterval(timer);
    };
  }, [active]);

  return { open, pending, tick };
}
