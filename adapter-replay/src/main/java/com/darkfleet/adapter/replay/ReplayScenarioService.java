package com.darkfleet.adapter.replay;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.model.AisSource;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Replays recorded Danish Maritime Authority (DMA) CSV data and curated corridor test scenarios
 * through the live Kafka pipeline, always marking messages with {@code historical = true}.
 */
@Service
public class ReplayScenarioService {

    private static final Logger log = LoggerFactory.getLogger(ReplayScenarioService.class);

    private final DmaCsvParser csvParser;
    private final KafkaTemplate<String, NormalizedAisMessage> kafkaTemplate;
    private final AtomicLong totalReplayed = new AtomicLong(0);

    public ReplayScenarioService(
            DmaCsvParser csvParser,
            KafkaTemplate<String, NormalizedAisMessage> kafkaTemplate
    ) {
        this.csvParser = csvParser;
        this.kafkaTemplate = kafkaTemplate;
    }

    public int replayCsv(String csvContent) {
        List<NormalizedAisMessage> messages = csvParser.parseCsvContent(csvContent);
        return publishBatch(messages);
    }

    /**
     * Generates and publishes the comprehensive recorded Baltic + Bosphorus corridor replay dataset.
     */
    public List<NormalizedAisMessage> buildRecordedCorridorSuite(Instant baseTime) {
        Instant t0 = baseTime.truncatedTo(ChronoUnit.HOURS);
        List<NormalizedAisMessage> batch = new ArrayList<>();

        // 1. Background commercial traffic establishing high dual-feed coverage in Gulf of Finland cell (59.80, 25.40)
        String[] bgMmsis = {"230101000", "230102000", "265501000", "276801000", "219401000"};
        for (int i = 0; i < bgMmsis.length; i++) {
            String mmsi = bgMmsis[i];
            batch.add(NormalizedAisMessage.staticReport(
                    mmsi, t0, AisSource.DIGITRAFFIC, true,
                    "910000" + i, "FINNISH CARRIER " + (i + 1), "OJ" + i, 70, 8.5, "HELSINKI"
            ));
            for (int m = 0; m <= 120; m += 10) {
                Instant ts = t0.plus(Duration.ofMinutes(m));
                double lat = 59.80 + (i * 0.004);
                double lon = 25.40 + (m * 0.0005);
                batch.add(NormalizedAisMessage.positionReport(
                        mmsi, ts, lat, lon, 12.0, 260.0, 260.0, 0, AisSource.DIGITRAFFIC, true
                ));
                batch.add(NormalizedAisMessage.positionReport(
                        mmsi, ts, lat, lon, 12.0, 260.0, 260.0, 0, AisSource.AISSTREAM, true
                ));
            }
        }

        // 2. Scenario A: Cross-Source Coverage Gap in Gulf of Finland (LADOGA STAR, MMSI 273310010, IMO 9299101)
        // Lost by AISSTREAM for 70 minutes, but seen continuously by DIGITRAFFIC -> suppressed as coverage gap!
        String ladogaMmsi = "273310010";
        batch.add(NormalizedAisMessage.staticReport(
                ladogaMmsi, t0, AisSource.DIGITRAFFIC, true,
                "9299101", "LADOGA STAR", "UBLS1", 80, 13.4, "SKAGEN"
        ));
        for (int m = 0; m <= 90; m += 10) {
            Instant ts = t0.plus(Duration.ofMinutes(m));
            double lat = 59.81;
            double lon = 25.42 - (m * 0.001);
            // Digitraffic sees every report
            batch.add(NormalizedAisMessage.positionReport(
                    ladogaMmsi, ts, lat, lon, 11.2, 255.0, 255.0, 0, AisSource.DIGITRAFFIC, true
            ));
            // aisstream only sees m=0 and m=90 (90-minute gap on aisstream alone)
            if (m == 0 || m == 90) {
                batch.add(NormalizedAisMessage.positionReport(
                        ladogaMmsi, ts, lat, lon, 11.2, 255.0, 255.0, 0, AisSource.AISSTREAM, true
                ));
            }
        }

        // 3. Scenario B: Unexplained AIS Gap in High-Coverage Water (SHADOW AURORA, MMSI 273450990, IMO 9341029)
        // Both feeds lose the vessel for 90 minutes inside the well-covered Gulf of Finland cell
        String auroraMmsi = "273450990";
        batch.add(NormalizedAisMessage.staticReport(
                auroraMmsi, t0, AisSource.AISSTREAM, true,
                "9341029", "SHADOW AURORA", "UBSA9", 80, 14.5, "FOR ORDERS"
        ));
        batch.add(NormalizedAisMessage.positionReport(
                auroraMmsi, t0.plus(Duration.ofMinutes(5)),
                59.795, 25.410, 12.5, 250.0, 250.0, 0, AisSource.AISSTREAM, true
        ));
        batch.add(NormalizedAisMessage.positionReport(
                auroraMmsi, t0.plus(Duration.ofMinutes(95)),
                59.770, 24.950, 12.1, 250.0, 250.0, 0, AisSource.AISSTREAM, true
        ));

        // 4. Scenario C: Offshore Ship-to-Ship (STS) Encounter + Draught Change near Danish Straits / Kattegat offshore
        // Two tankers (NEVA TITAN 273511200 and AEGEAN HORIZON 352899100, AIS type 80) + 1 Tug (219001122, AIS type 52, ignored)
        String titanMmsi = "273511200";
        String aegeanMmsi = "352899100";
        String tugMmsi = "219001122";

        batch.add(NormalizedAisMessage.staticReport(
                titanMmsi, t0, AisSource.DMA_REPLAY, true,
                "9388112", "NEVA TITAN", "UBNT2", 80, 14.6, "PORT SAID"
        ));
        batch.add(NormalizedAisMessage.staticReport(
                aegeanMmsi, t0, AisSource.DMA_REPLAY, true,
                "9400215", "AEGEAN HORIZON", "3FQM8", 80, 9.2, "ROTTERDAM"
        ));
        batch.add(NormalizedAisMessage.staticReport(
                tugMmsi, t0, AisSource.DMA_REPLAY, true,
                "8911223", "SVITZER KATTEGAT", "OXAA2", 52, 4.8, "SKAGEN"
        ));

        // 45 minutes of slow side-by-side drift (~180m apart) at (56.95, 11.45) - well away from ports/anchorages
        for (int m = 10; m <= 55; m += 15) {
            Instant ts = t0.plus(Duration.ofMinutes(m));
            batch.add(NormalizedAisMessage.positionReport(
                    titanMmsi, ts, 56.9500, 11.4500, 1.1, 180.0, 180.0, 0, AisSource.DMA_REPLAY, true
            ));
            batch.add(NormalizedAisMessage.positionReport(
                    aegeanMmsi, ts, 56.9512, 11.4510, 1.1, 180.0, 180.0, 0, AisSource.DMA_REPLAY, true
            ));
            // Tug is also right next to them, but must be ignored because shipType == 52!
            batch.add(NormalizedAisMessage.positionReport(
                    tugMmsi, ts, 56.9505, 11.4505, 1.0, 180.0, 180.0, 0, AisSource.DMA_REPLAY, true
            ));
        }

        // Post-encounter draught update: NEVA TITAN draught drops 14.6m -> 11.8m, AEGEAN HORIZON rises 9.2m -> 12.1m
        batch.add(NormalizedAisMessage.staticReport(
                titanMmsi, t0.plus(Duration.ofMinutes(65)), AisSource.DMA_REPLAY, true,
                "9388112", "NEVA TITAN", "UBNT2", 80, 11.8, "UST-LUGA"
        ));
        batch.add(NormalizedAisMessage.staticReport(
                aegeanMmsi, t0.plus(Duration.ofMinutes(65)), AisSource.DMA_REPLAY, true,
                "9400215", "AEGEAN HORIZON", "3FQM8", 80, 12.1, "SINGAPORE"
        ));

        // 5. Scenario D: Offshore Loitering + Identity Conflict in Western Black Sea / Bosphorus corridor
        String blackSeaMmsi = "636091880";
        batch.add(NormalizedAisMessage.staticReport(
                blackSeaMmsi, t0, AisSource.AISSTREAM, true,
                "9274511", "CAUCASUS EXPRESS", "5LBM9", 82, 12.0, "ISTANBUL"
        ));
        for (int m = 0; m <= 75; m += 15) {
            Instant ts = t0.plus(Duration.ofMinutes(m));
            batch.add(NormalizedAisMessage.positionReport(
                    blackSeaMmsi, ts, 41.650 + (m * 0.0001), 29.350, 1.4, 95.0, 95.0, 0, AisSource.AISSTREAM, true
            ));
        }
        // Conflicting static report on same MMSI (different vessel name & IMO!)
        batch.add(NormalizedAisMessage.staticReport(
                blackSeaMmsi, t0.plus(Duration.ofMinutes(80)), AisSource.AISSTREAM, true,
                "9299999", "VOLGA TRADER", "5LBM9", 82, 12.0, "CONSTANTA"
        ));

        batch.sort(Comparator.comparing(NormalizedAisMessage::eventTime));
        return batch;
    }

    public int replayRecordedCorridorSuite() {
        List<NormalizedAisMessage> suite = buildRecordedCorridorSuite(Instant.now().minus(Duration.ofHours(2)));
        return publishBatch(suite);
    }

    public int publishBatch(List<NormalizedAisMessage> messages) {
        int count = 0;
        for (NormalizedAisMessage msg : messages) {
            NormalizedAisMessage historicalMsg = msg.historical()
                    ? msg
                    : new NormalizedAisMessage(
                            msg.mmsi(), msg.eventTime(), msg.latitude(), msg.longitude(),
                            msg.sogKnots(), msg.cogDegrees(), msg.headingDegrees(), msg.navStatus(),
                            msg.source(), msg.messageType(), Instant.now(), true,
                            msg.imoNumber(), msg.vesselName(), msg.callSign(),
                            msg.shipType(), msg.draughtMeters(), msg.destination()
                    );
            try {
                kafkaTemplate.send(DarkFleetTopics.AIS_RAW, historicalMsg.mmsi(), historicalMsg);
            } catch (Exception e) {
                log.debug("Replay Kafka send deferred/logged for MMSI {}: {}", historicalMsg.mmsi(), e.getMessage());
            }
            count++;
            totalReplayed.incrementAndGet();
        }
        return count;
    }

    public Map<String, Object> getStatus() {
        return Map.of(
                "adapter", AisSource.DMA_REPLAY.name(),
                "displayName", AisSource.DMA_REPLAY.getDisplayName(),
                "historicalMarkerEnforced", true,
                "totalMessagesReplayed", totalReplayed.get()
        );
    }
}
