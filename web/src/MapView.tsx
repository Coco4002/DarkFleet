import { useEffect, useMemo, useRef, useState } from 'react';
import * as maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { MapboxOverlay } from '@deck.gl/mapbox';
import { IconLayer, PathLayer, ScatterplotLayer } from '@deck.gl/layers';
import { H3HexagonLayer } from '@deck.gl/geo-layers';
import type { Layer, PickingInfo } from '@deck.gl/core';
import type { CoverageCell, TrackPoint } from './api';
import type { Detection } from './analystApi';
import type { PortAnchorageCell } from './ports';
import { classOf } from './shipStyle';

/** A ship ready to draw: position already reckoned or interpolated for the moment being shown. */
export interface DrawnShip {
  mmsi: string;
  longitude: number;
  latitude: number;
  angle: number;
  shipType: number | null;
  stale: boolean;
  sogKnots?: number | null;
  navStatus?: number | null;
  h3Cell?: string | null;
  trail?: [number, number][];
}

interface Props {
  ships: DrawnShip[];
  coverage: CoverageCell[] | null;
  portsAndAnchorages: PortAnchorageCell[] | null;
  selectedMmsi: string | null;
  selectedTrail: TrackPoint[] | null;
  showTrails: boolean;
  /** Open detections, drawn only for signed-in analysts (the list is empty otherwise). */
  detections: Detection[];
  focus: { longitude: number; latitude: number; n: number } | null;
  onSelect: (mmsi: string | null) => void;
}

// Gulf of Finland and the Baltic out to the Danish straits.
const START_VIEW = { center: [22.5, 59.0] as [number, number], zoom: 5.2 };

const ARROW_SVG = `<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64">
<path d="M32 4 L50 56 L32 46 L14 56 Z" fill="white" stroke="black" stroke-width="2" stroke-linejoin="round"/></svg>`;
const ICON_ATLAS = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(ARROW_SVG)}`;
const ICON_MAPPING = { ship: { x: 0, y: 0, width: 64, height: 64, anchorY: 32, mask: true } };

// Free OpenStreetMap raster tiles (no API key; styled dark via CSS on .maplibregl-canvas so
// coastlines, cities and ports stay crisp without touching the deck.gl ship overlay).
const BASEMAP: maplibregl.StyleSpecification = {
  version: 8,
  sources: {
    osm: {
      type: 'raster',
      tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
      tileSize: 256,
      attribution: '© OpenStreetMap contributors',
    },
  },
  layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
};

function coverageColor(cell: CoverageCell): [number, number, number, number] {
  const sources = Object.entries(cell.windowPositionsBySource).filter(([, n]) => n > 0);
  const total = sources.reduce((sum, [, n]) => sum + n, 0);
  const alpha = Math.min(150, 40 + Math.log10(1 + total) * 30);
  if (sources.length >= 2) return [38, 198, 142, alpha]; // both feeds receive here
  if (sources.length === 1) return [255, 183, 77, alpha]; // only one feed
  return [120, 130, 140, 40];
}

function portAnchorageFillColor(cell: PortAnchorageCell): [number, number, number, number] {
  const intensity = Math.max(cell.totalStopped, cell.windowMaxStopped);
  const alpha = Math.min(185, 65 + Math.min(intensity, 15) * 8);
  if (cell.kind === 'port') {
    return [56, 189, 248, alpha]; // cyan-blue for shore/berth ports (moored)
  }
  return [245, 158, 11, alpha]; // amber-gold for water anchorages (at anchor)
}

function portAnchorageLineColor(cell: PortAnchorageCell): [number, number, number, number] {
  const alpha = cell.qualifiesAsPortOrAnchorage ? 245 : 160;
  if (cell.kind === 'port') {
    return [125, 211, 252, alpha];
  }
  return [251, 191, 36, alpha];
}

export function MapView({
  ships,
  coverage,
  portsAndAnchorages,
  selectedMmsi,
  selectedTrail,
  showTrails,
  detections,
  focus,
  onSelect,
}: Props) {
  const container = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const overlay = useRef<MapboxOverlay | null>(null);
  const onSelectRef = useRef(onSelect);
  onSelectRef.current = onSelect;
  const [hoveredPort, setHoveredPort] = useState<{ cell: PortAnchorageCell; x: number; y: number } | null>(null);

  useEffect(() => {
    if (!container.current) return;
    const m = new maplibregl.Map({
      container: container.current,
      style: BASEMAP,
      center: START_VIEW.center,
      zoom: START_VIEW.zoom,
      minZoom: 3,
      maxZoom: 14,
      attributionControl: { compact: true },
    });
    m.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right');
    const o = new MapboxOverlay({ interleaved: false, layers: [] });
    m.addControl(o as unknown as maplibregl.IControl);
    map.current = m;
    overlay.current = o;
    return () => {
      o.finalize();
      m.remove();
      map.current = null;
      overlay.current = null;
    };
  }, []);

  const trailData = useMemo(
    () => (selectedTrail ?? []).map((p) => [p.longitude, p.latitude] as [number, number]),
    [selectedTrail],
  );
  const selected = useMemo(() => ships.find((s) => s.mmsi === selectedMmsi) ?? null, [ships, selectedMmsi]);

  useEffect(() => {
    if (focus) map.current?.flyTo({ center: [focus.longitude, focus.latitude], zoom: Math.max(map.current.getZoom(), 9), duration: 1200 });
  }, [focus]);

  const located = useMemo(
    () => detections.filter((d) => d.latitude != null && d.longitude != null),
    [detections],
  );

  useEffect(() => {
    const layers: Layer[] = [];
    if (coverage) {
      layers.push(
        new H3HexagonLayer<CoverageCell>({
          id: 'coverage',
          data: coverage,
          getHexagon: (c) => c.h3Cell,
          getFillColor: coverageColor,
          filled: true,
          extruded: false,
          stroked: false,
          pickable: false,
        }),
      );
    }
    if (portsAndAnchorages) {
      layers.push(
        new H3HexagonLayer<PortAnchorageCell>({
          id: 'ports-anchorages',
          data: portsAndAnchorages,
          getHexagon: (c) => c.h3Cell,
          getFillColor: portAnchorageFillColor,
          getLineColor: portAnchorageLineColor,
          getLineWidth: (c) => (c.qualifiesAsPortOrAnchorage ? 2.5 : 1.5),
          lineWidthUnits: 'pixels',
          filled: true,
          extruded: false,
          stroked: true,
          pickable: true,
          onHover: (info: PickingInfo<PortAnchorageCell>) => {
            if (info.object) {
              setHoveredPort({ cell: info.object, x: info.x, y: info.y });
            } else {
              setHoveredPort(null);
            }
          },
        }),
      );
    } else {
      setHoveredPort(null);
    }
    if (showTrails) {
      layers.push(
        new PathLayer<DrawnShip>({
          id: 'historic-trails',
          data: ships.filter((s) => s.trail && s.trail.length > 1),
          getPath: (s) => s.trail!,
          getColor: (s) => [...classOf(s.shipType).color, 140] as [number, number, number, number],
          getWidth: 1.5,
          widthUnits: 'pixels',
        }),
      );
    }
    if (trailData.length > 1) {
      layers.push(
        new PathLayer<[number, number][]>({
          id: 'selected-trail',
          data: [trailData],
          getPath: (d) => d,
          getColor: [255, 255, 255, 200],
          getWidth: 2,
          widthUnits: 'pixels',
        }),
      );
    }
    layers.push(
      new IconLayer<DrawnShip>({
        id: 'ships',
        data: ships,
        iconAtlas: ICON_ATLAS,
        iconMapping: ICON_MAPPING,
        getIcon: () => 'ship',
        getPosition: (s) => [s.longitude, s.latitude],
        getAngle: (s) => -s.angle,
        getSize: (s) => (s.mmsi === selectedMmsi ? 26 : 16),
        sizeUnits: 'pixels',
        sizeMinPixels: 8,
        getColor: (s) => [...classOf(s.shipType).color, s.stale ? 90 : 255] as [number, number, number, number],
        pickable: true,
        updateTriggers: { getSize: selectedMmsi },
        onClick: (info) => {
          const ship = info.object as DrawnShip | undefined;
          onSelectRef.current(ship ? ship.mmsi : null);
        },
      }),
    );
    if (selected) {
      layers.push(
        new ScatterplotLayer<DrawnShip>({
          id: 'selected-ring',
          data: [selected],
          getPosition: (s) => [s.longitude, s.latitude],
          getRadius: 17,
          radiusUnits: 'pixels',
          stroked: true,
          filled: false,
          getLineColor: [255, 255, 255, 230],
          getLineWidth: 2,
          lineWidthUnits: 'pixels',
        }),
      );
    }
    if (located.length > 0) {
      layers.push(
        new ScatterplotLayer<Detection>({
          id: 'detections',
          data: located,
          getPosition: (d) => [d.longitude!, d.latitude!],
          getRadius: 14,
          radiusUnits: 'pixels',
          stroked: true,
          filled: true,
          getFillColor: [255, 82, 82, 50],
          getLineColor: [255, 82, 82, 255],
          getLineWidth: 2,
          lineWidthUnits: 'pixels',
        }),
      );
    }
    overlay.current?.setProps({ layers });
  }, [ships, coverage, portsAndAnchorages, showTrails, trailData, selected, selectedMmsi, located]);

  return (
    <>
      <div ref={container} className="map" />
      {hoveredPort && (
        <div
          className="hex-tooltip"
          style={{ left: hoveredPort.x + 12, top: hoveredPort.y + 12 }}
          role="tooltip"
        >
          <strong>
            {hoveredPort.cell.kind === 'port' ? 'Port (shore / berth)' : 'Anchorage (in water)'}
          </strong>
          <span>
            {hoveredPort.cell.moored > 0 && `${hoveredPort.cell.moored} moored`}
            {hoveredPort.cell.moored > 0 && hoveredPort.cell.anchored > 0 && ' · '}
            {hoveredPort.cell.anchored > 0 && `${hoveredPort.cell.anchored} at anchor`}
            {hoveredPort.cell.otherStopped > 0 &&
              `${hoveredPort.cell.moored > 0 || hoveredPort.cell.anchored > 0 ? ' · ' : ''}${hoveredPort.cell.otherStopped} stopped (<0.5 kn)`}
            {hoveredPort.cell.moored === 0 &&
              hoveredPort.cell.anchored === 0 &&
              hoveredPort.cell.otherStopped === 0 &&
              `${hoveredPort.cell.totalStopped} stopped in hour`}
          </span>
          {hoveredPort.cell.windowMaxStopped > hoveredPort.cell.totalStopped && (
            <span className="sub">24 h peak: {hoveredPort.cell.windowMaxStopped} stopped vessels</span>
          )}
          {hoveredPort.cell.qualifiesAsPortOrAnchorage && (
            <span className="tag">Data-driven port / anchorage cell (≥ 5 stopped)</span>
          )}
        </div>
      )}
    </>
  );
}
