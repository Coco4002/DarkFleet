package com.darkfleet.detection.support;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * When each identity (an IMO number, or an MMSI) was first and last reported. Two identities
 * conflict when their periods overlap: the older one was still being reported after the newer one
 * appeared. A clean switch (a corrected IMO, a vessel re-flagged to a new MMSI) never overlaps.
 */
public class Sightings {

    public Map<String, Period> periods = new HashMap<>();

    public record Period(Instant first, Instant last) {
    }

    public void record(String identity, Instant at) {
        periods.merge(identity, new Period(at, at), (a, b) -> new Period(
                a.first().isBefore(b.first()) ? a.first() : b.first(),
                a.last().isAfter(b.last()) ? a.last() : b.last()));
    }

    /** Forgets identities last reported before {@code cutoff}. */
    public void forgetBefore(Instant cutoff) {
        periods.values().removeIf(p -> p.last().isBefore(cutoff));
    }

    /** Identities whose period overlaps another's, sorted; empty when there is no conflict. */
    public List<String> conflicting() {
        TreeSet<String> result = new TreeSet<>();
        List<Map.Entry<String, Period>> entries = new ArrayList<>(periods.entrySet());
        for (int i = 0; i < entries.size(); i++) {
            for (int j = i + 1; j < entries.size(); j++) {
                Period a = entries.get(i).getValue();
                Period b = entries.get(j).getValue();
                if (!a.first().isAfter(b.last()) && !b.first().isAfter(a.last())) {
                    result.add(entries.get(i).getKey());
                    result.add(entries.get(j).getKey());
                }
            }
        }
        return List.copyOf(result);
    }

    @JsonIgnore
    public boolean isEmpty() {
        return periods.isEmpty();
    }
}
