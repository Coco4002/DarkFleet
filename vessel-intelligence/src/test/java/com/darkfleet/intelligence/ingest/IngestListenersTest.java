package com.darkfleet.intelligence.ingest;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Which database refusals skip a message and which fail the batch so it is retried. */
class IngestListenersTest {

    @Test
    void aRefusedValueSkipsOnlyThatMessage() {
        List<String> stored = new ArrayList<>();
        List<String> changed = IngestListeners.store(List.of("ok-1", "bad", "ok-2"), batch -> {
            if (batch.contains("bad")) {
                throw refusal("22021"); // invalid byte sequence: a NUL character
            }
            stored.addAll(batch);
            return batch;
        });
        assertEquals(List.of("ok-1", "ok-2"), stored);
        assertEquals(List.of("ok-1", "ok-2"), changed);
    }

    @Test
    void aMissingPartitionFailsTheBatchSoItIsRetried() {
        // a track point whose day retention dropped after the partition was ensured: no partition found
        assertThrows(DataIntegrityViolationException.class, () -> IngestListeners.store(List.of("point"), batch -> {
            throw refusal("23514");
        }));
    }

    private static DataIntegrityViolationException refusal(String sqlState) {
        return new DataIntegrityViolationException("refused", new SQLException("refused", sqlState));
    }
}
