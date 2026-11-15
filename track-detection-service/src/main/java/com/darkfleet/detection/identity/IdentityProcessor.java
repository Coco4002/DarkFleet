package com.darkfleet.detection.identity;

import com.darkfleet.common.model.Detection;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.detection.DetectionClock;
import com.darkfleet.detection.DetectionProperties;
import com.darkfleet.detection.Lookups;
import com.darkfleet.detection.support.Evidence;
import com.darkfleet.detection.support.Sightings;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The "one IMO number, several MMSIs" check, partitioned by IMO. Opens when two MMSIs broadcast the
 * same IMO in overlapping periods within the identity window (a re-flagged vessel switching MMSI
 * doesn't overlap), and closes once that is no longer so. The detection is published under the MMSI
 * whose report revealed the conflict.
 */
public class IdentityProcessor implements Processor<String, IdentityClaim, String, Detection> {

    public static final String STORE = "identity-by-imo";

    private final DetectionProperties properties;
    private final DetectionClock clock;
    private ProcessorContext<String, Detection> context;
    private KeyValueStore<String, Claims> imos;
    private Lookups lookups;

    public IdentityProcessor(DetectionProperties properties, DetectionClock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    public static class Claims {
        public Sightings mmsiSightings = new Sightings();
        public Map<String, String> vesselNames = new HashMap<>();
        public Detection conflict;
        /** The last report that showed the conflict; it ends there once it no longer shows. */
        public Instant conflictSeenAt;
    }

    @Override
    public void init(ProcessorContext<String, Detection> context) {
        this.context = context;
        this.imos = context.getStateStore(STORE);
        this.lookups = new Lookups(context, properties);
        clock.schedule(context, this::forgetOld);
    }

    @Override
    public void process(Record<String, IdentityClaim> record) {
        IdentityClaim claim = record.value();
        if (claim == null) {
            return;
        }
        DetectionRules rules = lookups.rules();
        Claims stored = imos.get(claim.imo());
        Claims claims = stored == null ? new Claims() : stored;
        claims.mmsiSightings.record(claim.mmsi(), claim.time());
        claims.mmsiSightings.forgetBefore(claim.time().minus(Duration.ofHours(rules.identityWindowHours())));
        claims.vesselNames.keySet().retainAll(claims.mmsiSightings.periods.keySet());
        if (claim.vesselName() != null) {
            claims.vesselNames.put(claim.mmsi(), claim.vesselName());
        }
        List<String> conflicting = claims.mmsiSightings.conflicting();
        if (conflicting.size() < 2) {
            endIfResolved(claims);
        } else if (claims.conflict == null) {
            String other = conflicting.stream().filter(m -> !m.equals(claim.mmsi())).findFirst().orElseThrow();
            List<String> names = conflicting.stream().filter(claims.vesselNames::containsKey)
                    .map(m -> m + ": " + claims.vesselNames.get(m)).toList();
            claims.conflict = new Detection("identity:imo:" + claim.imo() + ":" + claim.time().getEpochSecond(),
                    DetectionType.IDENTITY_INCONSISTENCY, DetectionStatus.OPEN, 1, claim.mmsi(), other, claim.time(), null,
                    null, null, null, rules.version(), Evidence.of(
                            "kind", "one IMO number, several MMSIs", "imoNumber", claim.imo(),
                            "mmsis", String.join(", ", conflicting), "vesselNames", String.join("; ", names)));
            context.forward(new Record<>(claim.mmsi(), claims.conflict, claim.time().toEpochMilli()));
        }
        if (conflicting.size() >= 2) {
            claims.conflictSeenAt = claims.conflictSeenAt == null || claim.time().isAfter(claims.conflictSeenAt)
                    ? claim.time() : claims.conflictSeenAt;
        }
        imos.put(claim.imo(), claims);
    }

    private void forgetOld(Instant now) {
        Duration window = Duration.ofHours(lookups.rules().identityWindowHours());
        List<KeyValue<String, Claims>> all = new ArrayList<>();
        try (KeyValueIterator<String, Claims> iterator = imos.all()) {
            iterator.forEachRemaining(all::add);
        }
        for (KeyValue<String, Claims> entry : all) {
            Claims claims = entry.value;
            int before = claims.mmsiSightings.periods.size();
            claims.mmsiSightings.forgetBefore(now.minus(window));
            if (claims.mmsiSightings.periods.size() == before) {
                continue;
            }
            endIfResolved(claims);
            if (claims.mmsiSightings.isEmpty()) {
                imos.delete(entry.key);
            } else {
                claims.vesselNames.keySet().retainAll(claims.mmsiSightings.periods.keySet());
                imos.put(entry.key, claims);
            }
        }
    }

    private void endIfResolved(Claims claims) {
        if (claims.conflict != null && claims.mmsiSightings.conflicting().size() < 2) {
            context.forward(new Record<>(claims.conflict.mmsi(), claims.conflict.closed(claims.conflictSeenAt,
                    Evidence.of("closeReason", "no longer conflicting")), claims.conflictSeenAt.toEpochMilli()));
            claims.conflict = null;
            claims.conflictSeenAt = null;
        }
    }
}
