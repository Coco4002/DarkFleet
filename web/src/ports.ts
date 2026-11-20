import { latLngToCell } from 'h3-js';
import type { CoverageCell } from './api';

export const H3_RESOLUTION = 6;
/** Speed threshold below which a vessel is considered stationary (matches CoverageTick.STOPPED_KNOTS in Track & Detection). */
export const STOPPED_KNOTS = 0.5;
/** Baseline minimum stopped vessels in an hour for Track & Detection to classify a cell as a port or anchorage. */
export const PORT_MIN_STOPPED_VESSELS = 5;

export const NAV_AT_ANCHOR = 1;
export const NAV_MOORED = 5;

export type StationaryKind = 'port' | 'anchorage';

export interface StationaryShipInput {
  mmsi: string;
  latitude: number;
  longitude: number;
  sogKnots: number | null;
  navStatus: number | null;
  h3Cell?: string | null;
}

export interface PortAnchorageCell {
  h3Cell: string;
  kind: StationaryKind;
  /** Ships moored at berth (navStatus === 5 and speed < 0.5 kn). */
  moored: number;
  /** Ships at anchor in water (navStatus === 1 and speed < 0.5 kn). */
  anchored: number;
  /** Stationary ships (< 0.5 kn) without moored/anchored navStatus. */
  otherStopped: number;
  /** Total currently stationary ships in this cell. */
  totalStopped: number;
  /** Highest hourly stopped vessel count over the 24 h coverage window (if coverage is loaded). */
  windowMaxStopped: number;
  /** True when the cell meets Track & Detection's port/anchorage threshold (>= 5 stopped vessels). */
  qualifiesAsPortOrAnchorage: boolean;
}

/**
 * True when a vessel is stationary according to both its navigational status and its GPS speed:
 * a ship claiming to be moored or at anchor while moving at >= 0.5 kn is underway, not stationary.
 */
export function isStationaryShip(sogKnots: number | null, navStatus: number | null): boolean {
  if (sogKnots != null && sogKnots >= STOPPED_KNOTS) {
    return false;
  }
  if (navStatus === NAV_MOORED || navStatus === NAV_AT_ANCHOR) {
    return true;
  }
  return sogKnots != null && sogKnots < STOPPED_KNOTS;
}

/**
 * Aggregates stationary ships and hourly H3 coverage into Port (moored at shore/berth) and
 * Anchorage (at anchor in water) cells.
 *
 * Distinction:
 * - A cell where stationary ships are predominantly moored (`navStatus = 5`, `moored > anchored` and `moored >= 1`)
 *   is classified as a **Port** (shore/berth).
 * - A cell where stationary ships are at anchor in water (`navStatus = 1`) or stopped offshore without berth status
 *   is classified as an **Anchorage**.
 */
export function buildPortAnchorageCells(
  ships: StationaryShipInput[],
  coverage: CoverageCell[] | null,
): PortAnchorageCell[] {
  const byCell = new Map<
    string,
    { moored: number; anchored: number; otherStopped: number; windowMaxStopped: number; hourlyStopped: number }
  >();

  for (const s of ships) {
    if (!isStationaryShip(s.sogKnots, s.navStatus)) continue;
    if (!Number.isFinite(s.latitude) || !Number.isFinite(s.longitude)) continue;
    const cell = s.h3Cell || latLngToCell(s.latitude, s.longitude, H3_RESOLUTION);
    let entry = byCell.get(cell);
    if (!entry) {
      entry = { moored: 0, anchored: 0, otherStopped: 0, windowMaxStopped: 0, hourlyStopped: 0 };
      byCell.set(cell, entry);
    }
    if (s.navStatus === NAV_MOORED) {
      entry.moored++;
    } else if (s.navStatus === NAV_AT_ANCHOR) {
      entry.anchored++;
    } else {
      entry.otherStopped++;
    }
  }

  if (coverage) {
    for (const c of coverage) {
      const peak = Math.max(c.stoppedVessels, c.windowMaxStoppedVessels);
      if (peak <= 0) continue;
      let entry = byCell.get(c.h3Cell);
      if (!entry) {
        if (peak < 2) continue;
        entry = { moored: 0, anchored: 0, otherStopped: 0, windowMaxStopped: 0, hourlyStopped: 0 };
        byCell.set(c.h3Cell, entry);
      }
      entry.windowMaxStopped = Math.max(entry.windowMaxStopped, c.windowMaxStoppedVessels);
      entry.hourlyStopped = Math.max(entry.hourlyStopped, c.stoppedVessels);
    }
  }

  const cells: PortAnchorageCell[] = [];
  for (const [h3Cell, e] of byCell.entries()) {
    const liveStopped = e.moored + e.anchored + e.otherStopped;
    const totalStopped = Math.max(liveStopped, e.hourlyStopped);
    const windowMaxStopped = Math.max(e.windowMaxStopped, totalStopped);
    // Show cells with at least 2 stationary ships, or any cell that has moored/anchored vessels or meets the 24h port threshold
    if (totalStopped < 1 && windowMaxStopped < 2) continue;

    const kind: StationaryKind = e.moored > 0 && e.moored >= e.anchored ? 'port' : 'anchorage';
    const qualifiesAsPortOrAnchorage =
      totalStopped >= PORT_MIN_STOPPED_VESSELS || windowMaxStopped >= PORT_MIN_STOPPED_VESSELS;

    cells.push({
      h3Cell,
      kind,
      moored: e.moored,
      anchored: e.anchored,
      otherStopped: e.otherStopped,
      totalStopped,
      windowMaxStopped,
      qualifiesAsPortOrAnchorage,
    });
  }

  cells.sort((a, b) => b.totalStopped - a.totalStopped || b.windowMaxStopped - a.windowMaxStopped);
  return cells;
}
