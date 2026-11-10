package com.darkfleet.common.model;

import java.time.LocalDate;
import java.util.List;

/**
 * Maritime sanctions record loaded from OpenSanctions maritime CSV (CC BY-NC 4.0).
 * Matched primarily by IMO number (survives flag/MMSI re-registration) and secondarily by MMSI.
 */
public record SanctionRecord(
        String entityId,
        String imoNumber,
        String mmsi,
        String vesselName,
        String flagState,
        List<String> datasets,
        List<String> programs,
        LocalDate listedDate,
        String summary
) {
}
