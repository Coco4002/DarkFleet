import { useEffect, useState } from 'react';
import { fetchVesselPanel, type LiveVessel, type TrackPoint, type VesselPanel } from './api';
import { classOf, navStatusLabel, shipTypeLabel } from './shipStyle';

interface Props {
  mmsi: string;
  live: LiveVessel | null;
  onClose: () => void;
  onTrail: (trail: TrackPoint[] | null) => void;
}

const fmt = (value: string | number | null | undefined, unit = '') =>
  value == null || value === '' ? '—' : `${value}${unit}`;

function flagEmoji(code: string | null): string {
  if (!code || !/^[A-Za-z]{2}$/.test(code)) return '';
  return String.fromCodePoint(...[...code.toUpperCase()].map((c) => 0x1f1e6 + c.charCodeAt(0) - 65));
}

export function ShipPanel({ mmsi, live, onClose, onTrail }: Props) {
  const [panel, setPanel] = useState<VesselPanel | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setPanel(null);
    setError(null);
    onTrail(null);
    fetchVesselPanel(mmsi, controller.signal)
      .then((p) => {
        setPanel(p);
        onTrail(p.trail);
      })
      .catch((e: Error) => {
        if (e.name !== 'AbortError') setError(e.message);
      });
    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mmsi]);

  const name = panel?.name ?? live?.name ?? null;
  const shipType = panel?.shipType ?? live?.shipType ?? null;
  const flagCode = panel?.flagCode ?? live?.flagCode ?? null;
  const flagCountry = panel?.flagCountry ?? live?.flagCountry ?? null;
  // the live snapshot keeps moving; the panel's own position is only the one it was opened with
  const position = live ?? panel?.lastPosition ?? null;
  const status = navStatusLabel(position?.navStatus ?? null);
  const color = classOf(shipType).color;

  return (
    <aside className="panel" aria-label="Ship details">
      <button className="close" onClick={onClose} aria-label="Close">×</button>
      {panel?.photo ? (
        <figure className="photo">
          <img src={panel.photo.imageUrl} alt={name ?? `Vessel ${mmsi}`} loading="lazy" referrerPolicy="no-referrer" />
          <figcaption>
            {panel.photo.descriptionUrl ? (
              <a href={panel.photo.descriptionUrl} target="_blank" rel="noopener noreferrer">
                {panel.photo.credit ?? 'Wikimedia Commons'}
              </a>
            ) : (
              panel.photo.credit ?? 'Wikimedia Commons'
            )}
            {panel.photo.license && (
              <>
                {' · '}
                {panel.photo.licenseUrl ? (
                  <a href={panel.photo.licenseUrl} target="_blank" rel="noopener noreferrer">{panel.photo.license}</a>
                ) : (
                  panel.photo.license
                )}
              </>
            )}
          </figcaption>
        </figure>
      ) : (
        <div className="silhouette" style={{ color: `rgb(${color.join(',')})` }} aria-hidden>
          <svg viewBox="0 0 64 64" width="64" height="64"><path d="M32 4 L50 56 L32 46 L14 56 Z" fill="currentColor" /></svg>
        </div>
      )}
      <h2>{name ?? `MMSI ${mmsi}`}</h2>
      <p className="sub">
        {flagEmoji(flagCode)} {flagCountry ?? 'Flag unknown'} · {shipTypeLabel(shipType)}
      </p>
      {error && <p className="warn">{error}</p>}
      <dl>
        <dt>MMSI</dt><dd>{mmsi}</dd>
        <dt>IMO</dt><dd>{fmt(panel?.imo ?? live?.imo)}</dd>
        <dt>Call sign</dt><dd>{fmt(panel?.callSign ?? live?.callSign)}</dd>
        <dt>Size</dt>
        <dd>
          {fmt(panel?.lengthMeters ?? live?.lengthMeters)}
          {(panel?.lengthMeters ?? live?.lengthMeters) != null && (panel?.beamMeters ?? live?.beamMeters) != null
            ? ` × ${panel?.beamMeters ?? live?.beamMeters} m`
            : ''}
        </dd>
        <dt>Draught</dt><dd>{fmt(panel?.draughtMeters ?? live?.draughtMeters, ' m')}</dd>
        <dt>Destination</dt><dd>{fmt(panel?.destination ?? live?.destination)}</dd>
        <dt>ETA</dt><dd>{panel?.eta ?? live?.eta ? new Date((panel?.eta ?? live?.eta)!).toLocaleString() : '—'}</dd>
        <dt>Speed</dt><dd>{fmt(position?.sogKnots, ' kn')}</dd>
        <dt>Course</dt><dd>{fmt(position?.cogDegrees, '°')}</dd>
        {status && (<><dt>Status</dt><dd>{status}</dd></>)}
        <dt>Last report</dt>
        <dd>{position?.time ? new Date(position.time).toLocaleTimeString() : 'not on the live map'}</dd>
      </dl>
      {panel && panel.trail == null && <p className="note">Track history is unavailable right now.</p>}
      {panel?.trail && panel.trail.length > 1 && <p className="note">White line: last 24 hours.</p>}
    </aside>
  );
}
