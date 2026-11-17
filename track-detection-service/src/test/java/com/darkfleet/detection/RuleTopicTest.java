package com.darkfleet.detection;

import com.darkfleet.common.model.DetectionRules;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Which rule topic each mode listens to, and where the baseline comes from. */
class RuleTopicTest {

    @Test
    void replayNeverReadsTheLiveRuleTopic() {
        String replay = describe(DetectionMode.REPLAY);
        assertTrue(replay.contains("[replay.rules.approved]"), replay);
        assertFalse(replay.contains("[rules.approved]"), "a rule approved while replaying would change live detection");

        String live = describe(DetectionMode.LIVE);
        assertTrue(live.contains("[rules.approved]"), live);
        assertFalse(live.contains("[replay.rules.approved]"), live);
    }

    @Test
    void baselineComesFromTheSharedFileAndCantBeConfigured() {
        assertEquals(DetectionRules.baseline(), properties(DetectionMode.LIVE, null).withSharedBaseline().baselineRules());
        DetectionProperties configured = properties(DetectionMode.LIVE, DetectionRules.baseline());
        assertThrows(IllegalStateException.class, configured::withSharedBaseline);
    }

    private static String describe(DetectionMode mode) {
        StreamsBuilder builder = new StreamsBuilder();
        DetectionTopology.build(builder, properties(mode, null).withSharedBaseline());
        return builder.build().describe().toString();
    }

    private static DetectionProperties properties(DetectionMode mode, DetectionRules baseline) {
        return new DetectionProperties(mode, Duration.ofMinutes(1), Duration.ofMinutes(2), Duration.ofMinutes(5),
                Duration.ofMinutes(1), 20, 3, Duration.ofHours(24), Duration.ofHours(48), Duration.ofMinutes(30),
                Duration.ofMinutes(10), baseline);
    }
}
