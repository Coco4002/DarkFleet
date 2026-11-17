package com.darkfleet.intelligence.api;

import com.darkfleet.intelligence.decisions.ProposalService;
import com.darkfleet.intelligence.decisions.Proposals.DecisionRequest;
import com.darkfleet.intelligence.decisions.Proposals.ProposalRequest;
import com.darkfleet.intelligence.decisions.Proposals.ProposalView;
import com.darkfleet.intelligence.decisions.Proposals.Status;
import com.darkfleet.intelligence.decisions.Proposals.Type;
import com.darkfleet.intelligence.web.Caller;
import com.darkfleet.intelligence.web.PageResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/proposals")
@Tag(name = "Proposals")
class ProposalController {

    private final ProposalService proposals;

    ProposalController(ProposalService proposals) {
        this.proposals = proposals;
    }

    @GetMapping
    @PreAuthorize(Access.READ)
    @Operation(summary = "Proposals, newest first; the analyst inbox is status=PENDING")
    PageResult<ProposalView> list(@RequestParam(required = false) Status status,
                                  @RequestParam(required = false) Type type,
                                  @RequestParam(defaultValue = "0") int page,
                                  @RequestParam(defaultValue = "" + PageResult.DEFAULT_SIZE) int size) {
        return proposals.list(status, type, page, size);
    }

    @GetMapping("/{id}")
    @PreAuthorize(Access.READ)
    ProposalView get(@PathVariable UUID id) {
        return proposals.get(id);
    }

    @PostMapping
    @PreAuthorize(Access.PROPOSE)
    @Operation(summary = "Propose a case, a watchlist change or a rule change. 201 when created; 200 with the "
            + "existing proposal when the same requestId was sent before. Nothing changes until an analyst approves.")
    ResponseEntity<ProposalView> create(@RequestBody ProposalRequest request, Authentication authentication) {
        ProposalService.Created result = proposals.create(request, Caller.of(authentication));
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.proposal());
    }

    @PostMapping("/{id}/approve")
    @PreAuthorize(Access.DECIDE)
    @Operation(summary = "Approve and apply. Rule changes need an administrator. 409 if already decided.")
    ProposalView approve(@PathVariable UUID id, @RequestBody(required = false) DecisionRequest decision,
                         Authentication authentication) {
        return proposals.approve(id, Caller.of(authentication), decision == null ? null : decision.note());
    }

    @PostMapping("/{id}/reject")
    @PreAuthorize(Access.DECIDE)
    @Operation(summary = "Reject, with a note saying why. 409 if already decided.")
    ProposalView reject(@PathVariable UUID id, @RequestBody DecisionRequest decision, Authentication authentication) {
        return proposals.reject(id, Caller.of(authentication), decision.note());
    }
}
