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
 * waiting for a decision. Refetched when the server announces a change (server-sent events), and each
 * time that stream (re)connects, so changes made while it was down are picked up.
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
        const [d, p] = await Promise.all([listDetections({ status: 'OPEN' }), listProposals('PENDING', 0)]);
        if (cancelled) return;
        setOpen(d.items);
        setPending(p.items.length + (p.hasMore ? 1 : 0));
        setTick((t) => t + 1);
      } catch {
        // session expired or server busy: the next change or reconnect tries again
      }
    }
    const es = new EventSource(analystEventsUrl);
    es.onopen = () => void load();
    es.addEventListener('detection', () => void load());
    es.addEventListener('proposal', () => void load());
    return () => {
      cancelled = true;
      es.close();
    };
  }, [active]);

  return { open, pending, tick };
}
