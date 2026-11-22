import { latLngToCell } from 'h3-js';
import type { CoverageCell } from './api';

export const H3_RESOLUTION = 6;
/** Speed threshold below which a vessel is considered stationary (matches CoverageTick.STOPPED_KNOTS in Track & Detection). */
export const STOPPED_KNOTS = 0.5;

export const NAV_AT_ANCHOR = 1;
export const NAV_MOORED = 5;

/** What the stopped ships in a cell say about themselves; "stopped" when they don't say or are split evenly. */
export type StationaryKind = 'port' | 'anchorage' | 'stopped';

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
  /** Stopped ships reporting "moored". */
  moored: number;
  /** Stopped ships reporting "at anchor". */
  anchored: number;
  /** Stopped ships reporting neither. */
  otherStopped: number;
  /** Ships stopped here now (live) or in the hour (history), whichever the data shows more of. */
  totalStopped: number;
  /** Track & Detection's count: most stopped vessels in one hour over the last 24 h. */
  windowMaxStopped: number;
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
 * Cells where ships stop, from the ships on the map and Track & Detection's coverage counts. Whether a cell
 * is a port or an anchorage is not decided here (Track & Detection owns that rule); the label only repeats
 * what most of its stopped ships report: moored, at anchor, or neither.
 */
export function buildPortAnchorageCells(
  ships: StationaryShipInput[],
  coverage: CoverageCell[] | null,
): PortAnchorageCell[] {
  const byCell = new Map<string, PortAnchorageCell>();
  const cellFor = (h3Cell: string) => {
    let cell = byCell.get(h3Cell);
    if (!cell) {
      cell = { h3Cell, kind: 'stopped', moored: 0, anchored: 0, otherStopped: 0, totalStopped: 0, windowMaxStopped: 0 };
      byCell.set(h3Cell, cell);
    }
    return cell;
  };

  for (const s of ships) {
    if (!isStationaryShip(s.sogKnots, s.navStatus)) continue;
    const cell = cellFor(s.h3Cell || latLngToCell(s.latitude, s.longitude, H3_RESOLUTION));
    if (s.navStatus === NAV_MOORED) cell.moored++;
    else if (s.navStatus === NAV_AT_ANCHOR) cell.anchored++;
    else cell.otherStopped++;
  }
  for (const c of coverage ?? []) {
    if (c.stoppedVessels === 0 && c.windowMaxStoppedVessels === 0) continue;
    const cell = cellFor(c.h3Cell);
    cell.totalStopped = c.stoppedVessels;
    cell.windowMaxStopped = c.windowMaxStoppedVessels;
  }

  const cells = [...byCell.values()];
  for (const cell of cells) {
    cell.totalStopped = Math.max(cell.totalStopped, cell.moored + cell.anchored + cell.otherStopped);
    cell.kind = cell.moored > cell.anchored ? 'port' : cell.anchored > cell.moored ? 'anchorage' : 'stopped';
  }
  return cells.sort((a, b) => b.totalStopped - a.totalStopped || b.windowMaxStopped - a.windowMaxStopped);
}
