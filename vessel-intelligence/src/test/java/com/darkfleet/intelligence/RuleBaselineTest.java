package com.darkfleet.intelligence;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.DetectionRules;
import com.darkfleet.intelligence.decisions.RuleSets;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Version 0 follows the shared baseline file, as Track &amp; Detection does. */
@SpringBootTest
class RuleBaselineTest extends IntegrationTest {

    @Autowired
    private RuleSets rules;

    @Test
    void versionZeroFollowsTheBaselineFileWhenItChanges() {
        // as stored by an older release whose baseline had another gap threshold
        DetectionRules old = new DetectionRules(0, 90, 5, 5, 3.0, 120, 500, 2.0, 120, 24, 0.4);
        jdbc.update("UPDATE rule_set SET rules = ?::jsonb WHERE version = 0", JsonSerde.mapper().writeValueAsString(old));

        rules.run(null);

        String stored = jdbc.queryForObject("SELECT rules::text FROM rule_set WHERE version = 0", String.class);
        assertEquals(DetectionRules.baseline(), JsonSerde.mapper().readValue(stored, DetectionRules.class));
        assertEquals(90, jdbc.queryForObject("""
                SELECT (before->>'gapMinutes')::int FROM audit_log
                WHERE entity_type = 'RULE_SET' AND entity_id = '0' ORDER BY id DESC LIMIT 1""", Integer.class));
    }
}
