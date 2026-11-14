package com.darkfleet.adapter.replay;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.model.NormalizedAisMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReplayRunServiceTest {

    @TempDir
    Path dataDir;

    private KafkaTemplate<String, Object> kafka;
    private ReplayRunService service;

    // The sample parses to 17 messages; three are exact repeats within the same second (one fishing
    // vessel, one Class B vessel and EAGLE KINABALU each send the same position twice), which the
    // publisher drops: 14 sent.
    private static final int EXPECTED_SENT = 14;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() throws IOException {
        kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        service = new ReplayRunService(kafka, dataDir);

        String sample = String.join("\n", DmaCsvParserTest.readSample()) + "\n";
        Files.writeString(dataDir.resolve("aisdk-2026-11-11.csv"), sample, StandardCharsets.UTF_8);
        try (OutputStream out = Files.newOutputStream(dataDir.resolve("aisdk-2026-11-11.zip"));
             ZipOutputStream zip = new ZipOutputStream(out)) {
            zip.putNextEntry(new ZipEntry("aisdk-2026-11-11.csv"));
            zip.write(sample.getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
    }

    private ReplayRunService.RunStatus runToEnd(ReplayRunService.ReplayRequest request) throws InterruptedException {
        service.start(request);
        for (int i = 0; i < 200 && service.status().state() == ReplayRunService.State.RUNNING; i++) {
            Thread.sleep(25);
        }
        return service.status();
    }

    @Test
    void replaysACsvIntoTheReplayTopicOnly() throws Exception {
        var status = runToEnd(new ReplayRunService.ReplayRequest("aisdk-2026-11-11.csv", null, null, null, null));
        assertEquals(ReplayRunService.State.FINISHED, status.state(), String.valueOf(status.error()));
        assertEquals(21, status.rowsRead());
        assertEquals(EXPECTED_SENT, status.messagesSent());
        assertEquals(EXPECTED_SENT, status.published(), "finished means every message was acknowledged");
        verify(kafka, never()).send(eq(DarkFleetTopics.AIS_RAW), anyString(), any());
    }

    @Test
    void theSameFileCanBeReplayedAgain() throws Exception {
        runToEnd(new ReplayRunService.ReplayRequest("aisdk-2026-11-11.csv", null, null, null, null));
        var second = runToEnd(new ReplayRunService.ReplayRequest("aisdk-2026-11-11.csv", null, null, null, null));
        assertEquals(ReplayRunService.State.FINISHED, second.state());
        assertEquals(EXPECTED_SENT, second.messagesSent(), "each run filters repeats on its own");
    }

    @SuppressWarnings("unchecked")
    @Test
    void aRunWithFailedSendsIsReportedAsFailed() throws Exception {
        when(kafka.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new org.apache.kafka.common.KafkaException("broker down")));
        var status = runToEnd(new ReplayRunService.ReplayRequest("aisdk-2026-11-11.csv", null, null, null, null));
        assertEquals(ReplayRunService.State.FAILED, status.state());
        assertTrue(status.error().contains("could not be sent"), status.error());
        assertEquals(0, status.published());
    }

    @Test
    void aVesselKeepsItsIdentityWhenTheWindowStartsAfterItsStaticReport() throws Exception {
        // BRAVIK's static row is at 00:00:18; its positions follow at 00:00:36, :47 and :56.
        var status = runToEnd(new ReplayRunService.ReplayRequest(
                "aisdk-2026-11-11.csv", Instant.parse("2026-11-11T00:00:30Z"), null, null, null));
        assertEquals(ReplayRunService.State.FINISHED, status.state());
        var sent = org.mockito.ArgumentCaptor.forClass(Object.class);
        verify(kafka, org.mockito.Mockito.atLeastOnce()).send(eq(DarkFleetTopics.AIS_REPLAY), eq("265074000"), sent.capture());
        List<NormalizedAisMessage> bravik = sent.getAllValues().stream().map(o -> (NormalizedAisMessage) o).toList();
        assertEquals(com.darkfleet.common.model.AisMessageType.STATIC_VOYAGE, bravik.getFirst().messageType());
        assertEquals("BRAVIK", bravik.getFirst().vesselName());
        assertEquals(4, bravik.size(), "identity + 3 positions");
    }

    @Test
    void readsTheZipFilesDmaPublishes() throws Exception {
        var status = runToEnd(new ReplayRunService.ReplayRequest("aisdk-2026-11-11.zip", null, null, null, null));
        assertEquals(ReplayRunService.State.FINISHED, status.state(), String.valueOf(status.error()));
        assertEquals(EXPECTED_SENT, status.messagesSent());
    }

    @Test
    void stopsAtTheEndOfTheTimeWindow() throws Exception {
        var status = runToEnd(new ReplayRunService.ReplayRequest(
                "aisdk-2026-11-11.csv", null, Instant.parse("2026-11-11T00:00:05Z"), null, null));
        assertEquals(ReplayRunService.State.FINISHED, status.state());
        assertTrue(status.lastEventTime().compareTo(Instant.parse("2026-11-11T00:00:05Z")) <= 0);
        assertTrue(status.messagesSent() < EXPECTED_SENT);
    }

    @Test
    void boundingBoxFiltersPositionsButKeepsStaticReports() throws Exception {
        // Kattegat box around BRAVIK and EAGLE KINABALU only
        var status = runToEnd(new ReplayRunService.ReplayRequest(
                "aisdk-2026-11-11.csv", null, null, null, List.of(56.9, 11.5, 57.7, 11.8)));
        assertEquals(ReplayRunService.State.FINISHED, status.state());
        var sent = org.mockito.ArgumentCaptor.forClass(Object.class);
        verify(kafka, org.mockito.Mockito.atLeastOnce()).send(eq(DarkFleetTopics.AIS_REPLAY), anyString(), sent.capture());
        for (Object o : sent.getAllValues()) {
            NormalizedAisMessage m = (NormalizedAisMessage) o;
            if (m.latitude() != null) {
                assertTrue(m.latitude() >= 56.9 && m.latitude() <= 57.7 && m.longitude() >= 11.5 && m.longitude() <= 11.8);
            }
        }
    }

    @Test
    void onlyPlainFileNamesInsideTheDataDirectoryAreAccepted() {
        assertThrows(IllegalArgumentException.class, () -> service.resolve("../secret.csv"));
        assertThrows(IllegalArgumentException.class, () -> service.resolve("..\\secret.csv"));
        assertThrows(IllegalArgumentException.class, () -> service.resolve("/etc/passwd"));
        assertThrows(IllegalArgumentException.class, () -> service.resolve("notes.txt"));
        assertThrows(IllegalArgumentException.class, () -> service.resolve("missing.csv"));
        assertEquals(dataDir.resolve("aisdk-2026-11-11.zip").toAbsolutePath().normalize(),
                service.resolve("aisdk-2026-11-11.zip"));
    }

    @Test
    void listsOnlyReplayFiles() throws Exception {
        Files.writeString(dataDir.resolve("notes.txt"), "x");
        assertEquals(List.of("aisdk-2026-11-11.csv", "aisdk-2026-11-11.zip"), service.listFiles());
    }

    @Test
    void rejectsABadBoundingBox() {
        assertThrows(IllegalArgumentException.class, () -> service.start(new ReplayRunService.ReplayRequest(
                "aisdk-2026-11-11.csv", null, null, null, List.of(58.0, 11.0, 57.0, 12.0))));
    }
}
