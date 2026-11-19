import { describe, expect, it } from 'vitest';
import type { TrackPoint } from './api';
import { deadReckon, iconAngle, MAX_EXTRAPOLATION_S, mergeTracks, positionAt } from './motion';

const T0 = Date.parse('2026-11-19T12:00:00Z');

function report(over: Partial<{ sogKnots: number | null; cogDegrees: number | null }> = {}) {
  return { time: '2026-11-19T12:00:00Z', latitude: 60, longitude: 25, sogKnots: 10, cogDegrees: 90, ...over };
}

function point(minutes: number, lon: number, lat = 60): TrackPoint {
  return {
    time: new Date(T0 + minutes * 60_000).toISOString(),
    latitude: lat,
    longitude: lon,
    sogKnots: 10,
    cogDegrees: 90,
    headingDegrees: null,
    navStatus: null,
    source: 'AISSTREAM',
  };
}

describe('deadReckon', () => {
  it('moves a ship east along its course from the report time', () => {
    const r = deadReckon(report(), T0 + 60_000);
    // 10 kn for 60 s = 308.7 m ≈ 0.00555° of longitude at 60°N
    expect(r.latitude).toBeCloseTo(60, 4);
    expect(r.longitude).toBeCloseTo(25 + 308.7 / (111_195 * Math.cos((60 * Math.PI) / 180)), 4);
    expect(r.stale).toBe(false);
  });

  it('stops extrapolating after the cap and marks the ship stale', () => {
    const capped = deadReckon(report(), T0 + MAX_EXTRAPOLATION_S * 1000);
    const later = deadReckon(report(), T0 + 600_000);
    expect(later.longitude).toBeCloseTo(capped.longitude, 9);
    expect(capped.stale).toBe(false);
    expect(later.stale).toBe(true);
  });

  it('keeps ships without usable speed or course where they were reported', () => {
    for (const r of [report({ sogKnots: null }), report({ cogDegrees: null }), report({ sogKnots: 0.2 })]) {
      const at = deadReckon(r, T0 + 60_000);
      expect(at.longitude).toBe(25);
      expect(at.latitude).toBe(60);
    }
  });

  it('does not move a ship backwards when the clock is behind the report', () => {
    expect(deadReckon(report(), T0 - 5000).longitude).toBe(25);
  });
});

describe('iconAngle', () => {
  it('prefers heading, ignoring 511 (not available), then course', () => {
    expect(iconAngle(45, 90)).toBe(45);
    expect(iconAngle(511, 90)).toBe(90);
    expect(iconAngle(null, null)).toBe(0);
  });
});

describe('positionAt', () => {
  const points = [point(0, 25.0), point(5, 25.1), point(10, 25.2)];

  it('interpolates between neighbouring points', () => {
    const p = positionAt(points, T0 + 7.5 * 60_000)!;
    expect(p.longitude).toBeCloseTo(25.15, 6);
    expect(p.trail.at(-1)).toEqual([p.longitude, p.latitude]);
    expect(p.trail.length).toBe(3);
  });

  it('draws a ship at its last point shortly after it, but not long after', () => {
    expect(positionAt(points, T0 + 15 * 60_000)!.longitude).toBe(25.2);
    expect(positionAt(points, T0 + 60 * 60_000)).toBeNull();
  });

  it('does not draw a ship before its first point by more than the allowed gap', () => {
    expect(positionAt(points, T0 - 10 * 60_000)!.longitude).toBe(25.0);
    expect(positionAt(points, T0 - 60 * 60_000)).toBeNull();
  });

  it('does not bridge a long silence', () => {
    const gap = [point(0, 25.0), point(120, 26.0)];
    expect(positionAt(gap, T0 + 60 * 60_000)).toBeNull();
  });
});

describe('mergeTracks', () => {
  it('joins hours in time order without duplicate points', () => {
    const track = (points: TrackPoint[]) => ({ mmsi: '1', name: null, shipType: null, lengthMeters: null, beamMeters: null, points });
    const merged = mergeTracks([[track([point(60, 1), point(65, 2)])], [track([point(55, 0), point(60, 1)])]]);
    expect(merged.get('1')!.points.map((p) => p.longitude)).toEqual([0, 1, 2]);
  });
});
