package com.darkfleet.intelligence.sanctions;

import com.darkfleet.intelligence.audit.AuditLog;
import com.darkfleet.intelligence.sanctions.MaritimeList.Listing;
import com.darkfleet.intelligence.web.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Imports OpenSanctions' maritime list. The whole list is replaced in one transaction, so readers see
 * either the old list or the new one. A failed download or a malformed file keeps the old list and is
 * recorded with the reason. An unchanged file (same SHA-256) is only recorded.
 */
@Component
public class SanctionImporter {

    private static final Logger log = LoggerFactory.getLogger(SanctionImporter.class);
    private static final long MAX_BYTES = 100L * 1024 * 1024;
    private static final String INSERT = """
            INSERT INTO sanction (id, name, imo, mmsi, topics, flag, countries, datasets, aliases, url, import_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO NOTHING""";

    public enum Status { IMPORTED, UNCHANGED, FAILED }

    public record Result(Status status, Integer vessels, Integer skipped, String message) {
    }

    private final SanctionsProperties properties;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final AuditLog audit;
    private final Clock clock;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20)).followRedirects(HttpClient.Redirect.NORMAL).build();
    private final ReentrantLock running = new ReentrantLock();

    public SanctionImporter(SanctionsProperties properties, JdbcTemplate jdbc, TransactionTemplate transaction,
                            AuditLog audit, Clock clock) {
        this.properties = properties;
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.audit = audit;
        this.clock = clock;
    }

    /** Hourly, and shortly after start-up: imports when the list is older than the maximum age. */
    @Scheduled(initialDelayString = "${darkfleet.intelligence.sanctions.first-check}", fixedDelayString = "PT1H")
    void refreshIfStale() {
        if (!properties.enabled()) {
            return;
        }
        Timestamp last = jdbc.queryForObject(
                "SELECT max(finished_at) FROM sanction_import WHERE status IN ('IMPORTED', 'UNCHANGED')", Timestamp.class);
        if (last == null || Duration.between(last.toInstant(), clock.instant()).compareTo(properties.maxAge()) >= 0) {
            importList("scheduler");
        }
    }

    /** Runs an import now. Only one at a time: a second caller gets 409. */
    public Result importList(String actor) {
        if (!properties.enabled()) {
            throw ApiException.conflict("No sanction list URL is configured (darkfleet.intelligence.sanctions.url)");
        }
        if (!running.tryLock()) {
            throw ApiException.conflict("An import is already running");
        }
        try {
            return run(actor);
        } finally {
            running.unlock();
        }
    }

    private Result run(String actor) {
        Instant started = clock.instant();
        String url = properties.url();
        try {
            byte[] content = download(url);
            String sha256 = sha256(content);
            String previousSha = jdbc.query("""
                    SELECT sha256 FROM sanction_import WHERE status IN ('IMPORTED', 'UNCHANGED')
                    ORDER BY id DESC LIMIT 1""", (rs, i) -> rs.getString(1)).stream().findFirst().orElse(null);
            if (sha256.equals(previousSha)) {
                record(url, started, Status.UNCHANGED, sha256, null, null, null);
                return new Result(Status.UNCHANGED, null, null, "The list has not changed since the last import");
            }

            MaritimeList.Parsed parsed;
            try (var reader = new InputStreamReader(new ByteArrayInputStream(content), StandardCharsets.UTF_8)) {
                parsed = MaritimeList.parse(reader);
            }
            Integer previous = jdbc.query("""
                    SELECT vessels FROM sanction_import WHERE status = 'IMPORTED' ORDER BY id DESC LIMIT 1""",
                    (rs, i) -> rs.getInt(1)).stream().findFirst().orElse(null);
            int vessels = parsed.listings().size();
            if (vessels == 0 || previous != null && vessels < previous * properties.minShare()) {
                throw new IOException("Only " + vessels + " vessels where the last import had " + previous
                        + "; the file looks truncated");
            }

            transaction.executeWithoutResult(status -> {
                long importId = record(url, started, Status.IMPORTED, sha256, vessels, parsed.skipped(), null);
                jdbc.update("DELETE FROM sanction");
                jdbc.batchUpdate(INSERT, parsed.listings(), 1000, (ps, l) -> {
                    ps.setString(1, l.id());
                    ps.setString(2, l.name());
                    ps.setString(3, l.imo());
                    ps.setArray(4, array(ps.getConnection(), l.mmsi()));
                    ps.setArray(5, array(ps.getConnection(), l.topics()));
                    ps.setString(6, l.flag());
                    ps.setArray(7, array(ps.getConnection(), l.countries()));
                    ps.setArray(8, array(ps.getConnection(), l.datasets()));
                    ps.setArray(9, array(ps.getConnection(), l.aliases()));
                    ps.setString(10, l.url());
                    ps.setLong(11, importId);
                });
                if (!"scheduler".equals(actor)) {
                    audit.record(actor, "IMPORT", "SANCTION_LIST", String.valueOf(importId), null,
                            Map.of("vessels", vessels, "skipped", parsed.skipped(), "sha256", sha256));
                }
            });
            log.info("Imported {} vessels from the maritime sanction list ({} rows skipped)", vessels, parsed.skipped());
            return new Result(Status.IMPORTED, vessels, parsed.skipped(), null);
        } catch (IOException | InterruptedException | RuntimeException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            log.warn("Sanction list import failed, keeping the previous list: {}", message);
            record(url, started, Status.FAILED, null, null, null, message);
            return new Result(Status.FAILED, null, null, message);
        }
    }

    private long record(String url, Instant started, Status status, String sha256, Integer vessels, Integer skipped,
                        String message) {
        return jdbc.queryForObject("""
                INSERT INTO sanction_import (source_url, started_at, finished_at, status, sha256, vessels, skipped, message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id""", Long.class,
                url, Timestamp.from(started), Timestamp.from(clock.instant()), status.name(), sha256, vessels, skipped,
                message);
    }

    private byte[] download(String url) throws IOException, InterruptedException {
        if (url.startsWith("http://") || url.startsWith("https://")) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMinutes(2))
                    .header("User-Agent", "DarkFleet vessel-intelligence").GET().build();
            HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            try (InputStream body = response.body()) {
                if (response.statusCode() != 200) {
                    throw new IOException("Download returned HTTP " + response.statusCode());
                }
                return readLimited(body);
            }
        }
        try (InputStream in = new DefaultResourceLoader().getResource(url).getInputStream()) {
            return readLimited(in);
        }
    }

    private static byte[] readLimited(InputStream in) throws IOException {
        byte[] content = in.readNBytes((int) MAX_BYTES + 1);
        if (content.length > MAX_BYTES) {
            throw new IOException("The file is larger than " + MAX_BYTES / (1024 * 1024) + " MB");
        }
        return content;
    }

    private static java.sql.Array array(java.sql.Connection connection, List<String> values) throws java.sql.SQLException {
        return connection.createArrayOf("text", values.toArray());
    }

    private static String sha256(byte[] content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
