import type { FeedHealth } from './api';
import type { LiveState } from './useLive';

const DAY_MS = 86_400_000;
export const HISTORY_DAYS = 7;
const STEP_MS = 5 * 60_000;

interface Props {
  nowMs: number;
  /** null = live */
  timeMs: number | null;
  playing: boolean;
  onTime: (ms: number | null) => void;
  onPlaying: (playing: boolean) => void;
}

export function TimeBar({ nowMs, timeMs, playing, onTime, onPlaying }: Props) {
  const min = Math.floor((nowMs - HISTORY_DAYS * DAY_MS + 60 * 60_000) / STEP_MS) * STEP_MS;
  const max = Math.floor(nowMs / STEP_MS) * STEP_MS;
  const value = Math.min(Math.max(timeMs ?? max, min), max);
  const live = timeMs == null;
  return (
    <div className="timebar">
      <button
        className="play"
        onClick={() => {
          if (playing) onPlaying(false);
          else {
            if (live) onTime(Math.max(min, nowMs - 6 * 3_600_000));
            onPlaying(true);
          }
        }}
        aria-label={playing ? 'Pause' : 'Play'}
      >
        {playing ? '❚❚' : '▶'}
      </button>
      <input
        type="range"
        min={min}
        max={max}
        step={STEP_MS}
        value={value}
        aria-label="Time, last 7 days"
        onChange={(e) => {
          const v = Number(e.target.value);
          onPlaying(false);
          onTime(v >= max ? null : v);
        }}
      />
      <span className="when">{live ? 'Live' : new Date(value).toLocaleString([], { weekday: 'short', hour: '2-digit', minute: '2-digit', day: 'numeric', month: 'short' })}</span>
      <button className={live ? 'liveBtn on' : 'liveBtn'} onClick={() => { onPlaying(false); onTime(null); }}>
        ● Live
      </button>
    </div>
  );
}

const STATE_TEXT: Record<LiveState, string> = {
  connecting: 'Connecting…',
  live: 'Live',
  paused: 'Paused (tab hidden)',
  reconnecting: 'Reconnecting…',
};

export function FeedBar({ feeds, state, ships }: { feeds: FeedHealth[]; state: LiveState; ships: number }) {
  return (
    <div className="feedbar" role="status">
      <span className={`dot ${state === 'live' ? 'ok' : 'bad'}`} /> {STATE_TEXT[state]} · {ships.toLocaleString()} ships
      {feeds.map((f) => (
        <span key={f.source} className={`feed ${f.stale ? 'stale' : ''}`} title={f.stale ? 'No fresh data from this feed' : 'Receiving'}>
          <span className={`dot ${f.stale ? 'bad' : 'ok'}`} /> {f.source === 'AISSTREAM' ? 'aisstream' : f.source === 'DIGITRAFFIC' ? 'Digitraffic' : f.source}
          {f.stale ? ' — stale' : ''}
        </span>
      ))}
    </div>
  );
}
