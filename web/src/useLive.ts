import { useEffect, useRef, useState } from 'react';
import { liveStreamUrl, type LiveSnapshot } from './api';

export type LiveState = 'connecting' | 'live' | 'paused' | 'reconnecting';

/**
 * The live snapshot over server-sent events. A hidden tab closes the stream (the server then has one
 * client less) and opens it again, with a fresh snapshot, when the tab is shown. EventSource retries
 * dropped connections itself; the state shows when that is happening.
 */
export function useLiveSnapshot(enabled: boolean): { snapshot: LiveSnapshot | null; state: LiveState; receivedAt: number } {
  const [snapshot, setSnapshot] = useState<LiveSnapshot | null>(null);
  const [state, setState] = useState<LiveState>('connecting');
  const [receivedAt, setReceivedAt] = useState(0);
  const source = useRef<EventSource | null>(null);

  useEffect(() => {
    if (!enabled) {
      return;
    }
    function open() {
      if (source.current) return;
      setState('connecting');
      const es = new EventSource(liveStreamUrl);
      es.addEventListener('snapshot', (event) => {
        setSnapshot(JSON.parse((event as MessageEvent<string>).data) as LiveSnapshot);
        setReceivedAt(Date.now());
        setState('live');
      });
      es.onerror = () => setState('reconnecting');
      source.current = es;
    }
    function close() {
      source.current?.close();
      source.current = null;
    }
    function onVisibility() {
      if (document.hidden) {
        close();
        setState('paused');
      } else {
        open();
      }
    }
    if (!document.hidden) open();
    else setState('paused');
    document.addEventListener('visibilitychange', onVisibility);
    return () => {
      document.removeEventListener('visibilitychange', onVisibility);
      close();
    };
  }, [enabled]);

  return { snapshot, state, receivedAt };
}
