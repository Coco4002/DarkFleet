// Dead reckoning and history interpolation. Pure functions, unit-tested.

import type { HistoryTrack, TrackPoint } from './api';

export const KNOT_MS = 0.514444;
/** Extrapolate at most this long; beyond it a ship is drawn where it was last reported, faded. */
export const MAX_EXTRAPOLATION_S = 120;
const EARTH_RADIUS_M = 6_371_000;
/** Below this a ship is treated as not moving (GPS noise on moored ships). */
const MIN_MOVING_KNOTS = 0.5;

export interface Reckoned {
  longitude: number;
  latitude: number;
  /** Older than the extrapolation cap: draw faded. */
  stale: boolean;
}

export interface Reportable {
  time: string;
  latitude: number;
  longitude: number;
  sogKnots: number | null;
  cogDegrees: number | null;
}

/**
 * Where a ship is probably now: its report moved along its course for the time since the report was
 * made (the position's own observation time, not the time of the snapshot), capped at two minutes.
 */
export function deadReckon(report: Reportable, nowMs: number, reportMs = Date.parse(report.time)): Reckoned {
  const ageS = Math.max(0, (nowMs - reportMs) / 1000);
  const stale = ageS > MAX_EXTRAPOLATION_S;
  const sog = report.sogKnots;
  const cog = report.cogDegrees;
  if (sog == null || cog == null || !(sog >= MIN_MOVING_KNOTS) || sog > 60 || cog < 0 || cog >= 360) {
    return { longitude: report.longitude, latitude: report.latitude, stale };
  }
  const distance = sog * KNOT_MS * Math.min(ageS, MAX_EXTRAPOLATION_S);
  const bearing = (cog * Math.PI) / 180;
  const lat1 = (report.latitude * Math.PI) / 180;
  const lon1 = (report.longitude * Math.PI) / 180;
  const angular = distance / EARTH_RADIUS_M;
  const lat2 = Math.asin(
    Math.sin(lat1) * Math.cos(angular) + Math.cos(lat1) * Math.sin(angular) * Math.cos(bearing),
  );
  const lon2 =
    lon1 +
    Math.atan2(
      Math.sin(bearing) * Math.sin(angular) * Math.cos(lat1),
      Math.cos(angular) - Math.sin(lat1) * Math.sin(lat2),
    );
  return { longitude: (lon2 * 180) / Math.PI, latitude: (lat2 * 180) / Math.PI, stale };
}

/** Heading if the transmitter has one (511 = not available), else course over ground, else north. */
export function iconAngle(heading: number | null, cog: number | null): number {
  if (heading != null && heading >= 0 && heading < 360) return heading;
  if (cog != null && cog >= 0 && cog < 360) return cog;
  return 0;
}

export interface HistoricPosition {
  longitude: number;
  latitude: number;
  angle: number;
  /** Positions the ship had in the minutes before: oldest first, ends at the position itself. */
  trail: [number, number][];
}

/** A ship that has no point this close (before or after) to the moment is not drawn. */
export const MAX_HISTORY_GAP_MS = 20 * 60_000;
const TRAIL_MS = 30 * 60_000;

/**
 * Position of one ship at a moment of the past from its coarse (5-minute) points: interpolated between
 * the neighbouring points, or null when there is no point close enough on both sides.
 */
export function positionAt(points: TrackPoint[], atMs: number): HistoricPosition | null {
  if (points.length === 0) return null;
  // points are time-ordered; find the first one at or after the moment
  let lo = 0;
  let hi = points.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (Date.parse(points[mid].time) < atMs) lo = mid + 1;
    else hi = mid;
  }
  const after = lo < points.length ? points[lo] : null;
  const before = lo > 0 ? points[lo - 1] : null;
  const beforeMs = before ? Date.parse(before.time) : -Infinity;
  const afterMs = after ? Date.parse(after.time) : Infinity;
  let longitude: number;
  let latitude: number;
  let angle: number;
  if (before && after && afterMs - beforeMs <= MAX_HISTORY_GAP_MS * 2) {
    const span = afterMs - beforeMs;
    const f = span <= 0 ? 0 : (atMs - beforeMs) / span;
    longitude = before.longitude + (after.longitude - before.longitude) * f;
    latitude = before.latitude + (after.latitude - before.latitude) * f;
    angle = iconAngle(before.headingDegrees, before.cogDegrees);
  } else {
    const nearest = before && atMs - beforeMs <= MAX_HISTORY_GAP_MS ? before : after && afterMs - atMs <= MAX_HISTORY_GAP_MS ? after : null;
    if (!nearest) return null;
    longitude = nearest.longitude;
    latitude = nearest.latitude;
    angle = iconAngle(nearest.headingDegrees, nearest.cogDegrees);
  }
  const trail: [number, number][] = [];
  for (let i = Math.max(0, lo - 8); i < lo; i++) {
    if (atMs - Date.parse(points[i].time) <= TRAIL_MS) trail.push([points[i].longitude, points[i].latitude]);
  }
  trail.push([longitude, latitude]);
  return { longitude, latitude, angle, trail };
}

export interface HistoricShip {
  track: HistoryTrack;
  position: HistoricPosition;
}

/** Merge the points of one ship from several hourly responses, ordered and without duplicates. */
export function mergeTracks(hours: (HistoryTrack[] | undefined)[]): Map<string, HistoryTrack> {
  const merged = new Map<string, HistoryTrack>();
  for (const tracks of hours) {
    if (!tracks) continue;
    for (const track of tracks) {
      const existing = merged.get(track.mmsi);
      if (!existing) {
        merged.set(track.mmsi, { ...track, points: [...track.points] });
      } else {
        existing.points.push(...track.points);
        existing.name = existing.name ?? track.name;
        existing.shipType = existing.shipType ?? track.shipType;
      }
    }
  }
  for (const track of merged.values()) {
    track.points.sort((a, b) => Date.parse(a.time) - Date.parse(b.time));
    track.points = track.points.filter((p, i, all) => i === 0 || p.time !== all[i - 1].time);
  }
  return merged;
}
