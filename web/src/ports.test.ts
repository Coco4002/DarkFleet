import { describe, expect, it } from 'vitest';
import { buildPortAnchorageCells, isStationaryShip, NAV_AT_ANCHOR, NAV_MOORED } from './ports';

describe('isStationaryShip', () => {
  it('requires GPS speed below 0.5 kn even when navStatus claims moored or at anchor', () => {
    expect(isStationaryShip(0.0, NAV_MOORED)).toBe(true);
    expect(isStationaryShip(0.3, NAV_AT_ANCHOR)).toBe(true);
    expect(isStationaryShip(8.5, NAV_MOORED)).toBe(false);
    expect(isStationaryShip(6.0, NAV_AT_ANCHOR)).toBe(false);
    expect(isStationaryShip(0.2, 0)).toBe(true);
    expect(isStationaryShip(12.0, 0)).toBe(false);
  });
});

describe('buildPortAnchorageCells', () => {
  const PORT_CELL = '86089b1a7ffffff';
  const ANCHOR_CELL = '861135847ffffff';

  it('labels cells by what most of their stopped ships report', () => {
    const cells = buildPortAnchorageCells(
      [
        { mmsi: '1', latitude: 60.15, longitude: 24.92, sogKnots: 0.0, navStatus: NAV_MOORED, h3Cell: PORT_CELL },
        { mmsi: '2', latitude: 60.15, longitude: 24.92, sogKnots: 0.1, navStatus: NAV_MOORED, h3Cell: PORT_CELL },
        { mmsi: '3', latitude: 59.88, longitude: 26.12, sogKnots: 0.2, navStatus: NAV_AT_ANCHOR, h3Cell: ANCHOR_CELL },
        { mmsi: '4', latitude: 59.88, longitude: 26.12, sogKnots: 0.1, navStatus: NAV_AT_ANCHOR, h3Cell: ANCHOR_CELL },
        { mmsi: '5', latitude: 59.88, longitude: 26.12, sogKnots: 0.0, navStatus: NAV_AT_ANCHOR, h3Cell: ANCHOR_CELL },
        // Underway ship claiming to be moored is ignored
        { mmsi: '6', latitude: 59.88, longitude: 26.12, sogKnots: 11.2, navStatus: NAV_MOORED, h3Cell: ANCHOR_CELL },
      ],
      [
        {
          h3Cell: PORT_CELL,
          hourStart: '2026-11-20T08:00:00Z',
          positionsBySource: { DIGITRAFFIC: 300 },
          vessels: 8,
          stoppedVessels: 5,
          windowPositionsBySource: { DIGITRAFFIC: 6000 },
          windowMaxStoppedVessels: 7,
        },
      ],
    );

    const port = cells.find((c) => c.h3Cell === PORT_CELL)!;
    expect(port.kind).toBe('port');
    expect(port.moored).toBe(2);
    expect(port.windowMaxStopped).toBe(7);

    const anchorage = cells.find((c) => c.h3Cell === ANCHOR_CELL)!;
    expect(anchorage.kind).toBe('anchorage');
    expect(anchorage.anchored).toBe(3);
    expect(anchorage.moored).toBe(0);
  });

  it('does not guess a label for cells known only from coverage counts', () => {
    const cells = buildPortAnchorageCells([], [
      {
        h3Cell: PORT_CELL,
        hourStart: '2026-11-20T08:00:00Z',
        positionsBySource: { DIGITRAFFIC: 300 },
        vessels: 8,
        stoppedVessels: 6,
        windowPositionsBySource: { DIGITRAFFIC: 6000 },
        windowMaxStoppedVessels: 6,
      },
    ]);
    expect(cells).toHaveLength(1);
    expect(cells[0].kind).toBe('stopped');
    expect(cells[0].totalStopped).toBe(6);
  });
});
