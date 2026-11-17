package com.darkfleet.common.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DetectionRulesTest {

    @Test
    void baselineIsVersionZeroWithTheAgreedThresholds() {
        DetectionRules baseline = DetectionRules.baseline();
        assertEquals(new DetectionRules(0, 60, 5, 5, 3.0, 120, 500, 2.0, 120, 24, 0.4), baseline);
    }
}
