package com.darkfleet.intelligence.decisions;

import com.darkfleet.intelligence.read.Mappers;
import com.darkfleet.intelligence.read.Views.WatchlistView;
import com.darkfleet.intelligence.web.ApiException;
import com.darkfleet.intelligence.web.PageResult;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/** Vessels analysts want to keep an eye on. Changed only by approving a proposal. */
@Component
public class Watchlist {

    private static final Pattern MMSI = Pattern.compile("\\d{9}");
    private static final Pattern IMO = Pattern.compile("\\d{7}");

    private final NamedParameterJdbcTemplate jdbc;
    private final Clock clock;

    public Watchlist(NamedParameterJdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    public List<String> problems(Proposals.WatchlistPayload p) {
        List<String> problems = new ArrayList<>();
        if (p.action() == null) {
            problems.add("action is ADD or REMOVE");
        }
        if (p.mmsi() == null && p.imo() == null) {
            problems.add("mmsi or imo is required");
        }
        if (p.mmsi() != null && !MMSI.matcher(p.mmsi()).matches()) {
            problems.add("mmsi must have 9 digits");
        }
        if (p.imo() != null && !IMO.matcher(p.imo()).matches()) {
            problems.add("imo must have 7 digits");
        }
        if (p.reason() == null || p.reason().isBlank() || p.reason().length() > 2000) {
            problems.add("reason is required, at most 2000 characters");
        }
        return problems;
    }

    /** Applies an approved change inside the approval's transaction; 409 when it no longer applies. */
    @Transactional(propagation = Propagation.MANDATORY)
    UUID apply(UUID proposalId, Proposals.WatchlistPayload p, String decidedBy) {
        var params = new MapSqlParameterSource("mmsi", p.mmsi()).addValue("imo", p.imo())
                .addValue("proposal", proposalId).addValue("by", decidedBy)
                .addValue("at", Timestamp.from(clock.instant()));
        return switch (p.action()) {
            case ADD -> {
                UUID id = UUID.randomUUID();
                try {
                    jdbc.update("""
                            INSERT INTO watchlist_entry (id, mmsi, imo, reason, active, proposal_id, added_by, added_at)
                            VALUES (:id, :mmsi, :imo, :reason, true, :proposal, :by, :at)""",
                            params.addValue("id", id).addValue("reason", p.reason().strip()));
                } catch (DuplicateKeyException e) {
                    throw ApiException.conflict("The vessel is already on the watchlist");
                }
                yield id;
            }
            case REMOVE -> {
                List<UUID> removed = jdbc.queryForList("""
                        UPDATE watchlist_entry SET active = false, removed_by = :by, removed_at = :at,
                                                   removal_proposal_id = :proposal, updated_at = now()
                        WHERE active AND (mmsi = :mmsi OR imo = :imo) RETURNING id""", params, UUID.class);
                if (removed.isEmpty()) {
                    throw ApiException.conflict("The vessel is not on the watchlist");
                }
                yield removed.getFirst();
            }
        };
    }

    @Transactional(readOnly = true)
    public PageResult<WatchlistView> list(boolean includeRemoved, int page, int size) {
        PageResult.check(page, size);
        return PageResult.of(jdbc.query("SELECT * FROM watchlist_entry"
                        + (includeRemoved ? "" : " WHERE active") + " ORDER BY added_at DESC, id"
                        + PageResult.limit(page, size), new MapSqlParameterSource(), Mappers.WATCHLIST),
                page, size);
    }
}
