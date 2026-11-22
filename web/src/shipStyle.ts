export type RGB = [number, number, number];

export interface ShipClass {
  key: string;
  label: string;
  color: RGB;
}

export const CLASSES: ShipClass[] = [
  { key: 'tanker', label: 'Tanker', color: [255, 112, 67] },
  { key: 'cargo', label: 'Cargo', color: [66, 165, 245] },
  { key: 'passenger', label: 'Passenger', color: [102, 217, 140] },
  { key: 'fishing', label: 'Fishing', color: [255, 213, 79] },
  { key: 'tug', label: 'Tug / special', color: [186, 104, 200] },
  { key: 'pleasure', label: 'Pleasure craft', color: [77, 208, 225] },
  { key: 'other', label: 'Other / unknown', color: [158, 170, 184] },
];

const BY_KEY = new Map(CLASSES.map((c) => [c.key, c]));

/** AIS ship-and-cargo type code (ITU-R M.1371 table 53) → display class. */
export function classOf(shipType: number | null): ShipClass {
  const key = (() => {
    if (shipType == null) return 'other';
    if (shipType >= 80 && shipType <= 89) return 'tanker';
    if (shipType >= 70 && shipType <= 79) return 'cargo';
    if (shipType >= 60 && shipType <= 69) return 'passenger';
    if (shipType === 30) return 'fishing';
    if (shipType === 31 || shipType === 32 || shipType === 52 || (shipType >= 50 && shipType <= 59)) return 'tug';
    if (shipType === 36 || shipType === 37) return 'pleasure';
    return 'other';
  })();
  return BY_KEY.get(key)!;
}

export function shipTypeLabel(shipType: number | null): string {
  if (shipType == null) return 'Unknown';
  const base = classOf(shipType).label;
  return `${base} (${shipType})`;
}

/** Names of AIS navigational status codes. */
export function navStatusLabel(code: number | null): string | null {
  switch (code) {
    case 0: return 'Under way using engine';
    case 1: return 'At anchor';
    case 2: return 'Not under command';
    case 3: return 'Restricted manoeuvrability';
    case 4: return 'Constrained by draught';
    case 5: return 'Moored';
    case 6: return 'Aground';
    case 7: return 'Engaged in fishing';
    case 8: return 'Under way sailing';
    case 15: return null;
    default: return null;
  }
}
