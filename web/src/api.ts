// Types mirror the public map-gateway API (/api/v1/map/**, see its OpenAPI document).

export type Source = 'AISSTREAM' | 'DIGITRAFFIC' | string;

export interface LiveVessel {
  mmsi: string;
  time: string;
  latitude: number;
  longitude: number;
  sogKnots: number | null;
  cogDegrees: number | null;
  headingDegrees: number | null;
  navStatus: number | null;
  source: Source;
  h3Cell: string | null;
  name: string | null;
  imo: string | null;
  callSign: string | null;
  flagCode: string | null;
  flagCountry: string | null;
  shipType: number | null;
  lengthMeters: number | null;
  beamMeters: number | null;
  draughtMeters: number | null;
  destination: string | null;
  eta: string | null;
}

export interface FeedHealth {
  source: Source;
  connected: boolean;
  stale: boolean;
  emittedAt: string;
  lastMessageAt: string | null;
  messagesInInterval: number;
  positionsInInterval: number;
}

export interface LiveSnapshot {
  emittedAt: string;
  feeds: FeedHealth[];
  vessels: LiveVessel[];
}

export interface TrackPoint {
  time: string;
  latitude: number;
  longitude: number;
  sogKnots: number | null;
  cogDegrees: number | null;
  headingDegrees: number | null;
  navStatus: number | null;
  source: string;
}

export interface VesselPhoto {
  imageUrl: string;
  descriptionUrl: string | null;
  author: string | null;
  license: string | null;
  licenseUrl: string | null;
  credit: string | null;
}

export interface VesselPanel {
  mmsi: string;
  name: string | null;
  imo: string | null;
  callSign: string | null;
  flagCode: string | null;
  flagCountry: string | null;
  shipType: number | null;
  lengthMeters: number | null;
  beamMeters: number | null;
  draughtMeters: number | null;
  destination: string | null;
  eta: string | null;
  staticReportedAt: string | null;
  lastPosition: TrackPoint | null;
  trail: TrackPoint[] | null;
  photo: VesselPhoto | null;
}

export interface HistoryTrack {
  mmsi: string;
  name: string | null;
  shipType: number | null;
  lengthMeters: number | null;
  beamMeters: number | null;
  points: TrackPoint[];
}

export interface HistoryHour {
  hourStart: string;
  hourEnd: string;
  vessels: HistoryTrack[];
}

export interface CoverageCell {
  h3Cell: string;
  hourStart: string;
  positionsBySource: Record<string, number>;
  vessels: number;
  stoppedVessels: number;
  windowPositionsBySource: Record<string, number>;
  windowMaxStoppedVessels: number;
}

const BASE = '/api/v1/map';

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(BASE + path, { signal, headers: { Accept: 'application/json' } });
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const problem = await response.json();
      detail = problem.detail ?? detail;
    } catch {
      // not a problem document
    }
    throw new Error(`${response.status}: ${detail}`);
  }
  return (await response.json()) as T;
}

export const fetchVesselPanel = (mmsi: string, signal?: AbortSignal) =>
  getJson<VesselPanel>(`/vessels/${encodeURIComponent(mmsi)}`, signal);

export const fetchHistoryHour = (hourIso: string, signal?: AbortSignal) =>
  getJson<HistoryHour>(`/history/tracks?hour=${encodeURIComponent(hourIso)}`, signal);

export const fetchCoverageHour = (hourIso: string, signal?: AbortSignal) =>
  getJson<CoverageCell[]>(`/coverage?hour=${encodeURIComponent(hourIso)}`, signal);

export const liveStreamUrl = `${BASE}/live`;
