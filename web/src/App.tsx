import { useEffect, useMemo, useState } from 'react';
import { fetchLatestCoverage, type CoverageCell, type HistoryTrack, type TrackPoint } from './api';
import { getSession, logout, type Detection, type Session } from './analystApi';
import { AnalystPanel } from './AnalystPanel';
import { FeedBar, TimeBar } from './Bars';
import { LoginDialog } from './LoginDialog';
import { useAnalystFeed } from './useAnalystFeed';
import { coverageCache, historyCache, hourOf } from './hourCache';
import { MapView, type DrawnShip } from './MapView';
import { deadReckon, iconAngle, mergeTracks, positionAt, MAX_HISTORY_GAP_MS } from './motion';
import { buildPortAnchorageCells } from './ports';
import { ShipPanel } from './ShipPanel';
import { CLASSES } from './shipStyle';
import { useLiveSnapshot } from './useLive';

const HOUR_MS = 3_600_000;

/** Hours of history needed to place ships at a moment: its own, and a neighbour when it is near the edge. */
function hoursFor(timeMs: number): number[] {
  const hour = hourOf(timeMs);
  const hours = [hour];
  if (timeMs - hour < MAX_HISTORY_GAP_MS) hours.unshift(hour - HOUR_MS);
  if (hour + HOUR_MS - timeMs < MAX_HISTORY_GAP_MS && hour + HOUR_MS <= Date.now()) hours.push(hour + HOUR_MS);
  return hours;
}

export function App() {
  const [timeMs, setTimeMs] = useState<number | null>(null);
  const [playing, setPlaying] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [selectedTrail, setSelectedTrail] = useState<TrackPoint[] | null>(null);
  const [showCoverage, setShowCoverage] = useState(false);
  const [showPorts, setShowPorts] = useState(false);
  const [nowMs, setNowMs] = useState(Date.now());
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [tracks, setTracks] = useState<Map<string, HistoryTrack>>(new Map());
  const [coverage, setCoverage] = useState<CoverageCell[] | null>(null);

  const historic = timeMs != null;
  const { snapshot, state } = useLiveSnapshot(true);

  // analyst session: asked once on load (this also hands the browser its CSRF cookie)
  const [session, setSession] = useState<Session | null>(null);
  const [loginOpen, setLoginOpen] = useState(false);
  const [analystOpen, setAnalystOpen] = useState(false);
  const [focus, setFocus] = useState<{ longitude: number; latitude: number; n: number } | null>(null);
  useEffect(() => {
    getSession().then(setSession).catch(() => setSession({ authenticated: false, username: null, roles: [] }));
  }, []);
  const signedIn = session?.authenticated === true;
  const analyst = useAnalystFeed(session);

  function expire() {
    setSession({ authenticated: false, username: null, roles: [] });
    setAnalystOpen(false);
    setLoginOpen(true);
  }
  function show(d: Detection) {
    setFocus((f) => ({ longitude: d.longitude!, latitude: d.latitude!, n: (f?.n ?? 0) + 1 }));
    setSelected(d.mmsi);
  }

  // one clock for the whole page: moves ships once a second while live
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  // play: ten minutes of data per second
  useEffect(() => {
    if (!playing || timeMs == null) return;
    const id = setInterval(() => {
      setTimeMs((t) => {
        if (t == null) return t;
        const next = t + 60_000;
        if (next >= Date.now() - 5 * 60_000) {
          setPlaying(false);
          return null;
        }
        return next;
      });
    }, 100);
    return () => clearInterval(id);
  }, [playing, timeMs == null]);

  // history for the hour under the slider (and neighbours), debounced while scrubbing
  const hoursKey = timeMs == null ? '' : hoursFor(timeMs).join(',');
  useEffect(() => {
    if (!hoursKey) {
      setHistoryError(null);
      return;
    }
    let cancelled = false;
    const timer = setTimeout(async () => {
      const hours = hoursKey.split(',').map(Number);
      const results = await Promise.allSettled(hours.map((h) => historyCache.get(h)));
      if (cancelled) return;
      const failed = results.find((r) => r.status === 'rejected');
      setHistoryError(failed ? 'History is unavailable right now.' : null);
      setTracks(
        mergeTracks(results.map((r) => (r.status === 'fulfilled' ? r.value.vessels : undefined))),
      );
      // fetch the hour after in the background so playing doesn't stall
      const last = hours[hours.length - 1];
      if (last + HOUR_MS <= Date.now()) void historyCache.get(last + HOUR_MS).catch(() => undefined);
    }, 150);
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [hoursKey]);

  // coverage hexes: live, the newest published hour (refreshed each minute); else the hour under the slider
  const needCoverage = showCoverage || showPorts;
  const coverageHour = timeMs == null ? null : hourOf(timeMs);
  const coverageMinute = timeMs == null ? Math.floor(nowMs / 60_000) : 0;
  useEffect(() => {
    if (!needCoverage) {
      setCoverage(null);
      return;
    }
    let cancelled = false;
    (coverageHour == null ? fetchLatestCoverage() : coverageCache.get(coverageHour))
      .then((cells) => !cancelled && setCoverage(cells))
      .catch(() => !cancelled && setCoverage([]));
    return () => {
      cancelled = true;
    };
  }, [needCoverage, coverageHour, coverageMinute]);

  const liveParsed = useMemo(
    () => (snapshot?.vessels ?? []).map((v) => ({ v, ms: Date.parse(v.time) })),
    [snapshot],
  );

  const ships: DrawnShip[] = useMemo(() => {
    if (timeMs == null) {
      return liveParsed.map(({ v, ms }) => {
        const r = deadReckon(v, nowMs, ms);
        return {
          mmsi: v.mmsi,
          longitude: r.longitude,
          latitude: r.latitude,
          angle: iconAngle(v.headingDegrees, v.cogDegrees),
          shipType: v.shipType,
          stale: r.stale,
          sogKnots: v.sogKnots,
          navStatus: v.navStatus,
          h3Cell: v.h3Cell,
        };
      });
    }
    const drawn: DrawnShip[] = [];
    for (const track of tracks.values()) {
      const p = positionAt(track.points, timeMs);
      if (p) {
        drawn.push({
          mmsi: track.mmsi,
          longitude: p.longitude,
          latitude: p.latitude,
          angle: p.angle,
          shipType: track.shipType,
          stale: false,
          sogKnots: p.sogKnots,
          navStatus: p.navStatus,
          trail: p.trail,
        });
      }
    }
    return drawn;
  }, [timeMs, liveParsed, nowMs, tracks]);

  const portsAndAnchorages = useMemo(() => {
    if (!showPorts) return null;
    const stationaryInput =
      timeMs == null
        ? (snapshot?.vessels ?? []).map((v) => ({
            mmsi: v.mmsi,
            latitude: v.latitude,
            longitude: v.longitude,
            sogKnots: v.sogKnots,
            navStatus: v.navStatus,
            h3Cell: v.h3Cell,
          }))
        : ships.map((s) => ({
            mmsi: s.mmsi,
            latitude: s.latitude,
            longitude: s.longitude,
            sogKnots: s.sogKnots ?? null,
            navStatus: s.navStatus ?? null,
            h3Cell: s.h3Cell ?? null,
          }));
    return buildPortAnchorageCells(stationaryInput, coverage);
  }, [showPorts, timeMs, snapshot, ships, coverage]);

  const liveSelected = useMemo(
    () => (selected ? (snapshot?.vessels.find((v) => v.mmsi === selected) ?? null) : null),
    [selected, snapshot],
  );

  return (
    <div className="app">
      <MapView
        ships={ships}
        coverage={showCoverage ? coverage : null}
        portsAndAnchorages={portsAndAnchorages}
        selectedMmsi={selected}
        selectedTrail={selectedTrail}
        showTrails={historic}
        detections={signedIn ? analyst.open : []}
        focus={focus}
        onSelect={setSelected}
      />
      <header className="topbar">
        <h1>DarkFleet</h1>
        <FeedBar feeds={snapshot?.feeds ?? []} state={state} ships={historic ? ships.length : (snapshot?.vessels.length ?? 0)} />
        <label className="toggle">
          <input type="checkbox" checked={showCoverage} onChange={(e) => setShowCoverage(e.target.checked)} /> Reception coverage
        </label>
        <label className="toggle">
          <input type="checkbox" checked={showPorts} onChange={(e) => setShowPorts(e.target.checked)} /> Ports &amp; anchorages
        </label>
        {signedIn ? (
          <span className="who">
            <button onClick={() => setAnalystOpen((o) => !o)}>
              Analyst{analyst.pending > 0 && <span className="badge">{analyst.pending}</span>}
            </button>
            <button
              onClick={() => {
                void logout().finally(() => {
                  setSession({ authenticated: false, username: null, roles: [] });
                  setAnalystOpen(false);
                });
              }}
              title={`Signed in as ${session?.username}`}
            >
              Sign out
            </button>
          </span>
        ) : (
          session && <button className="who" onClick={() => setLoginOpen(true)}>Analyst login</button>
        )}
      </header>
      {historic && (
        <div className="banner">
          History view{historyError ? ` — ${historyError}` : ''}
        </div>
      )}
      <ul className={`legend ${analystOpen ? 'hidden' : ''}`} aria-label="Ship types">
        {CLASSES.map((c) => (
          <li key={c.key}><i style={{ background: `rgb(${c.color.join(',')})` }} />{c.label}</li>
        ))}
        {showCoverage && (
          <>
            <li><i className="hex both" />Both feeds receive</li>
            <li><i className="hex one" />One feed only</li>
          </>
        )}
        {showPorts && (
          <>
            <li><i className="hex port" />Mostly moored ships</li>
            <li><i className="hex anchorage" />Mostly ships at anchor</li>
            <li><i className="hex stopped" />Stopped ships</li>
          </>
        )}
      </ul>
      {signedIn && analystOpen && session && (
        <AnalystPanel
          session={session}
          tick={analyst.tick}
          pending={analyst.pending}
          onClose={() => setAnalystOpen(false)}
          onShow={show}
          onSessionExpired={expire}
        />
      )}
      {loginOpen && (
        <LoginDialog
          onCancel={() => setLoginOpen(false)}
          onDone={(s) => {
            setSession(s);
            setLoginOpen(false);
            setAnalystOpen(true);
          }}
        />
      )}
      {selected && (
        <ShipPanel
          key={selected}
          mmsi={selected}
          live={liveSelected}
          onClose={() => {
            setSelected(null);
            setSelectedTrail(null);
          }}
          onTrail={setSelectedTrail}
        />
      )}
      <TimeBar nowMs={nowMs} timeMs={timeMs} playing={playing} onTime={setTimeMs} onPlaying={setPlaying} />
      <footer className="credits">
        AIS: aisstream.io · Source: Fintraffic / digitraffic.fi, license CC 4.0 BY · Sanctions: OpenSanctions, CC BY-NC 4.0 · Photos: Wikimedia Commons · Map © OpenStreetMap contributors
      </footer>
    </div>
  );
}
