package com.darkfleet.intelligence;

import com.darkfleet.intelligence.audit.AuditLog;
import com.darkfleet.intelligence.ingest.TrackPartitions;
import com.darkfleet.intelligence.sanctions.SanctionImporter;
import com.darkfleet.intelligence.sanctions.SanctionsProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Importing OpenSanctions' maritime list and matching it to vessels by IMO number or MMSI. */
class SanctionApiTest extends IntegrationTest {

    @Autowired
    private TrackPartitions partitions;
    @Autowired
    private TransactionTemplate transaction;
    @Autowired
    private AuditLog audit;

    @Test
    void importMatchAndKeepTheListWhenANewFileLooksWrong() throws Exception {
        Seed seed = new Seed(jdbc, partitions);
        seed.vessel("273200001", "9308065", "EVENTIN");       // listed by IMO: sanctioned and shadow fleet
        seed.vessel("613000001", null, "RUNNER");             // listed by one of two MMSIs: shadow fleet only

        mvc.perform(post(ANALYST_KEY, "/api/v1/sanctions/import", null)).andExpect(status().isForbidden());
        mvc.perform(post(ADMIN_KEY, "/api/v1/sanctions/import", null))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("IMPORTED"))
                .andExpect(jsonPath("$.vessels").value(4))
                .andExpect(jsonPath("$.skipped").value(3));
        mvc.perform(post(ADMIN_KEY, "/api/v1/sanctions/import", null))
                .andExpect(jsonPath("$.status").value("UNCHANGED"));
        mvc.perform(get(SERVICE_KEY, "/api/v1/sanctions/status"))
                .andExpect(jsonPath("$.vessels").value(4))
                .andExpect(jsonPath("$.lastSuccess.status").value("UNCHANGED"));
        mvc.perform(get(ADMIN_KEY, "/api/v1/audit?entityType=SANCTION_LIST"))
                .andExpect(jsonPath("$.items[0].actor").value("admin"))
                .andExpect(jsonPath("$.items[0].after.vessels").value(4));

        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels/{mmsi}", "273200001"))
                .andExpect(jsonPath("$.listings[*].id", contains("eu-fsf-eventin")))
                .andExpect(jsonPath("$.listings[0].topics", contains("sanction", "mare.shadow")));
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels?sanctioned=true"))
                .andExpect(jsonPath("$.items[*].mmsi", hasItem("273200001")))
                .andExpect(jsonPath("$.items[*].mmsi", not(hasItem("613000001"))));
        mvc.perform(get(ANALYST_KEY, "/api/v1/vessels?listed=true"))
                .andExpect(jsonPath("$.items[*].mmsi", hasItem("613000001")));

        // a file with a fraction of the vessels is refused; the loaded list stays
        var truncated = new SanctionImporter(
                new SanctionsProperties("classpath:sanctions/truncated.csv", Duration.ofHours(24), 0.5),
                jdbc, transaction, audit, Clock.systemUTC());
        SanctionImporter.Result result = truncated.importList("admin");
        assertEquals(SanctionImporter.Status.FAILED, result.status());
        assertEquals(4L, jdbc.queryForObject("SELECT count(*) FROM sanction", Long.class));
        mvc.perform(get(SERVICE_KEY, "/api/v1/sanctions/status"))
                .andExpect(jsonPath("$.lastAttempt.status").value("FAILED"))
                .andExpect(jsonPath("$.lastSuccess.status").value("UNCHANGED"));

        // as many rows as before, but all the same vessel: refused too
        var repeated = new SanctionImporter(
                new SanctionsProperties("classpath:sanctions/repeated.csv", Duration.ofHours(24), 0.5),
                jdbc, transaction, audit, Clock.systemUTC());
        assertEquals(SanctionImporter.Status.FAILED, repeated.importList("admin").status());
        assertEquals(4L, jdbc.queryForObject("SELECT count(*) FROM sanction", Long.class));
    }
}
