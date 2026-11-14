"""Coverage test report for DarkFleet.

Reads an export of ais.raw (JSON lines, optionally gzipped; see export.sh) and prints:
  1. volume per feed (messages, ships, rate, latency)
  2. ships per area and feed, and how many are tankers / cargo ships
  3. feed silences (whole feed quiet for > 2 min: reconnects or outages)
  4. per-ship gaps per feed and area (> 30 min, > 2 h)
  5. overlap of the two feeds in the Gulf of Finland, and how many gaps on one feed
     were covered by the other

Usage:  python coverage_stats.py data/ais-raw-<time>.jsonl.gz [--status data/status.jsonl]
Only the Python standard library is used.
"""

import argparse
import gzip
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime

# (name, min_lat, max_lat, min_lon, max_lon), first match wins
AREAS = [
    ("Gulf of Finland", 59.0, 60.9, 22.5, 30.5),
    ("Danish straits", 53.5, 58.0, 9.0, 13.0),
    ("Baltic proper", 53.5, 60.9, 9.0, 30.5),
    ("Bosphorus/Black Sea", 40.8, 44.5, 27.5, 32.5),
]
GAP_MIN = 30 * 60          # per-ship gap threshold (s)
GAP_LONG = 2 * 60 * 60     # long gap (s)
FEED_SILENCE = 2 * 60      # whole-feed silence threshold (s)


def area_of(lat, lon):
    for name, a, b, c, d in AREAS:
        if a <= lat <= b and c <= lon <= d:
            return name
    return "Outside"


def ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def ship_class(t):
    if t is None:
        return "unknown type"
    if 80 <= t <= 89:
        return "tanker"
    if 70 <= t <= 79:
        return "cargo"
    return "other"


def open_input(path):
    return gzip.open(path, "rt", encoding="utf-8") if path.endswith(".gz") else open(path, encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("export")
    ap.add_argument("--status", help="data/status.jsonl from the status poller")
    args = ap.parse_args()

    msgs = Counter()                     # (source, kind) -> count
    bad = 0
    first, last = {}, {}                 # source -> ts
    latency = defaultdict(list)          # source -> sampled ingest - event delay (s)
    feed_times = defaultdict(set)        # source -> seconds with at least one message, for silences
    ships = defaultdict(set)             # (area, source) -> mmsi
    ships_by_source = defaultdict(set)
    ship_type = {}                       # mmsi -> AIS ship type (latest seen)
    has_imo = set()
    hourly = defaultdict(set)            # (hour, area, source) -> mmsi
    last_pos = {}                        # (source, mmsi) -> (ts, area)
    gaps = []                            # (source, mmsi, area, start, end)
    gof_minutes = defaultdict(set)       # (source, mmsi) -> minute buckets seen in Gulf of Finland
    intervals = defaultdict(Counter)     # source -> {seconds between consecutive positions of a ship: count}
    n = 0

    with open_input(args.export) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                m = json.loads(line)
                src = m["source"]
                kind = m["messageType"]
                mmsi = m["mmsi"]
                t = ts(m["eventTime"])
                ing = ts(m["ingestionTime"]) if m.get("ingestionTime") else t
            except (ValueError, KeyError, TypeError):
                bad += 1
                continue
            n += 1
            msgs[(src, kind)] += 1
            first[src] = min(first.get(src, ing), ing)
            last[src] = max(last.get(src, ing), ing)
            if n % 50 == 0:
                latency[src].append(ing - t)
            feed_times[src].add(int(ing))

            if kind == "STATIC_VOYAGE":
                if m.get("shipType") is not None:
                    ship_type[mmsi] = m["shipType"]
                if m.get("imoNumber"):
                    has_imo.add(mmsi)
                continue

            lat, lon = m.get("latitude"), m.get("longitude")
            if lat is None or lon is None:
                continue
            area = area_of(lat, lon)
            ships[(area, src)].add(mmsi)
            ships_by_source[src].add(mmsi)
            hourly[(int(t // 3600), area, src)].add(mmsi)
            if area == "Gulf of Finland":
                gof_minutes[(src, mmsi)].add(int(t // 60))

            key = (src, mmsi)
            prev = last_pos.get(key)
            if prev is None or t > prev[0]:
                if prev is not None:
                    intervals[src][int(t - prev[0])] += 1
                    if t - prev[0] >= GAP_MIN:
                        gaps.append((src, mmsi, prev[1], prev[0], t))
                last_pos[key] = (t, area)

    if n == 0:
        sys.exit("No messages found in export.")

    sources = sorted(first)
    hr = lambda s: f"{s / 3600:.1f} h"
    rec_start, rec_end = min(first.values()), max(last.values())
    # Every hour of the recording counts, including hours with no data (outages); the partial
    # first and last hours are left out once there are enough full hours.
    all_hours = list(range(int(rec_start // 3600), int(rec_end // 3600) + 1))
    if len(all_hours) > 3:
        all_hours = all_hours[1:-1]

    print("=" * 72)
    print(f"DarkFleet coverage test report   ({n:,} messages, {bad} unreadable lines)")
    print("=" * 72)

    print("\n1. Volume per feed")
    for s in sources:
        span = max(last[s] - first[s], 1)
        total = msgs[(s, "POSITION_REPORT")] + msgs[(s, "STATIC_VOYAGE")]
        lat_med = statistics.median(latency[s]) if latency[s] else float("nan")
        print(f"  {s:<12} {total:>11,} msgs over {hr(span)}  ({total / span:.1f}/s)  "
              f"positions {msgs[(s, 'POSITION_REPORT')]:,}  static {msgs[(s, 'STATIC_VOYAGE')]:,}  "
              f"ships {len(ships_by_source[s]):,}  median delay {lat_med:.1f} s")

    print("\n   Time between two position reports of the same ship")
    for s in sources:
        c = intervals[s]
        total = sum(c.values())
        if not total:
            continue
        acc, marks = 0, {}
        for sec in sorted(c):
            acc += c[sec]
            for q in (0.5, 0.9):
                if q not in marks and acc >= q * total:
                    marks[q] = sec
        print(f"  {s:<12} median {marks[0.5]} s   90% within {marks[0.9]} s")

    print("\n2. Ships per area and feed (unique MMSIs; type from static messages of either feed)")
    print(f"  {'area':<22}{'feed':<13}{'ships':>7}{'tanker':>8}{'cargo':>7}{'other':>7}{'unknown':>9}{'avg/hour':>10}")
    for area in [a[0] for a in AREAS] + ["Outside"]:
        for s in sources:
            ids = ships.get((area, s))
            if not ids:
                continue
            c = Counter(ship_class(ship_type.get(x)) for x in ids)
            avg = statistics.mean(len(hourly.get((h, area, s), ())) for h in all_hours)
            print(f"  {area:<22}{s:<13}{len(ids):>7}{c['tanker']:>8}{c['cargo']:>7}{c['other']:>7}"
                  f"{c['unknown type']:>9}{avg:>10.0f}")
    pos_ships = set().union(*ships_by_source.values())
    print(f"  Ships with an IMO number from static messages: {len(has_imo & pos_ships):,} of {len(pos_ships):,}")

    print(f"\n3. Feed silences (no message at all for > {FEED_SILENCE // 60} min)")
    for s in sources:
        # Recording start and end are included, so an outage at either end is counted too.
        times = [int(rec_start)] + sorted(feed_times[s]) + [int(rec_end)]
        silences = [(a, b) for a, b in zip(times, times[1:]) if b - a > FEED_SILENCE]
        total = sum(b - a for a, b in silences)
        print(f"  {s:<12} {len(silences)} silences, {total / 60:.0f} min total")
        for a, b in silences[:10]:
            print(f"      {datetime.utcfromtimestamp(a):%Y-%m-%d %H:%M} -> {datetime.utcfromtimestamp(b):%H:%M} UTC "
                  f"({(b - a) / 60:.0f} min)")

    print(f"\n4. Per-ship gaps per feed (position reports > {GAP_MIN // 60} min apart; area = where the gap began)")
    print(f"  {'area':<22}{'feed':<13}{'gaps':>7}{'> 2 h':>7}{'ships':>7}{'tanker gaps':>13}")
    by = defaultdict(list)
    for g in gaps:
        by[(g[2], g[0])].append(g)
    for (area, s), gs in sorted(by.items()):
        tankers = sum(1 for g in gs if ship_class(ship_type.get(g[1])) == "tanker")
        print(f"  {area:<22}{s:<13}{len(gs):>7}{sum(1 for g in gs if g[4] - g[3] >= GAP_LONG):>7}"
              f"{len({g[1] for g in gs}):>7}{tankers:>13}")
    print("  (Includes ships that moored, left the area or simply sent fewer reports; this is volume, not detections.)")

    print("\n5. Two-feed overlap in the Gulf of Finland")
    if {"AISSTREAM", "DIGITRAFFIC"} <= set(sources):
        a = ships.get(("Gulf of Finland", "AISSTREAM"), set())
        d = ships.get(("Gulf of Finland", "DIGITRAFFIC"), set())
        print(f"  ships: both {len(a & d):,}   aisstream only {len(a - d):,}   digitraffic only {len(d - a):,}")
        both_h, a_h, d_h = [], [], []
        hours = all_hours
        for h in hours:
            ah = hourly.get((h, "Gulf of Finland", "AISSTREAM"), set())
            dh = hourly.get((h, "Gulf of Finland", "DIGITRAFFIC"), set())
            both_h.append(len(ah & dh)); a_h.append(len(ah - dh)); d_h.append(len(dh - ah))
        if hours:
            print(f"  per hour (avg): both {statistics.mean(both_h):.0f}   aisstream only {statistics.mean(a_h):.0f}"
                  f"   digitraffic only {statistics.mean(d_h):.0f}")
        for s, other in (("AISSTREAM", "DIGITRAFFIC"), ("DIGITRAFFIC", "AISSTREAM")):
            gs = [g for g in gaps if g[0] == s and g[2] == "Gulf of Finland"]
            covered = 0
            for _, mmsi, _, start, end in gs:
                seen = gof_minutes.get((other, mmsi), ())
                lo, hi = int(start // 60) + 1, int(end // 60) - 1
                if any(lo <= mnt <= hi for mnt in seen):
                    covered += 1
            if gs:
                print(f"  {s} gaps in the Gulf: {len(gs)}, of which {covered} ({100 * covered / len(gs):.0f}%) "
                      f"had the ship still reported by {other}")
    else:
        print("  Only one feed present; no overlap to compute.")

    if args.status:
        print("\n6. Adapter status (last poll)")
        latest = {}
        with open(args.status, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                latest[rec["url"]] = rec
        for url, rec in latest.items():
            st = rec.get("status") or {}
            print(f"  {st.get('adapter', url)}: connected={st.get('connected')} received={st.get('messagesReceived')} "
                  f"published={st.get('messagesPublished')} duplicates={st.get('duplicatesDropped')} "
                  f"(polled {rec['polledAt']})")
        print("  Reconnects: docker compose logs adapter-aisstream adapter-digitraffic | grep -ci reconnect")


if __name__ == "__main__":
    main()
