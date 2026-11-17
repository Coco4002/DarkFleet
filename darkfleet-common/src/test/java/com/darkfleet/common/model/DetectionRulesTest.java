package com.darkfleet.common.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DetectionRulesTest {

    @Test
    void baselineIsVersionZeroWithTheAgreedThresholds() {
        DetectionRules baseline = DetectionRules.baseline();
        assertEquals(new DetectionRules(0, 60, 5, 5, 3.0, 120, 500, 2.0, 120, 24, 0.4), baseline);
    }

    @Test
    void thresholdsMustBeFinite() {
        assertThrows(IllegalArgumentException.class,
                () -> new DetectionRules(1, 60, 5, 5, Double.NaN, 120, 500, 2.0, 120, 24, 0.4));
        assertThrows(IllegalArgumentException.class,
                () -> new DetectionRules(1, 60, 5, 5, 3.0, 120, Double.POSITIVE_INFINITY, 2.0, 120, 24, 0.4));
    }
}
