package com.darkfleet.intelligence.api;

import com.darkfleet.intelligence.decisions.Cases;
import com.darkfleet.intelligence.decisions.Proposals.DecisionRequest;
import com.darkfleet.intelligence.read.Views.CaseSummary;
import com.darkfleet.intelligence.web.Caller;
import com.darkfleet.intelligence.web.PageResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@RequestMapping("/api/v1/cases")
@Tag(name = "Cases")
class CaseController {

    private final Cases cases;

    CaseController(Cases cases) {
        this.cases = cases;
    }

    @GetMapping
    @PreAuthorize(Access.READ)
    @Operation(summary = "Cases, newest first; status is OPEN or CLOSED. Cases are opened by approving a proposal.")
    PageResult<CaseSummary> list(@RequestParam(required = false) String status,
                                 @RequestParam(required = false) String mmsi,
                                 @RequestParam(defaultValue = "0") int page,
                                 @RequestParam(defaultValue = "" + PageResult.DEFAULT_SIZE) int size) {
        return cases.list(status, mmsi, page, size);
    }

    @GetMapping("/{id}")
    @PreAuthorize(Access.READ)
    @Operation(summary = "A case with its detections and the evidence copied when it opened")
    Cases.CaseDetail get(@PathVariable UUID id) {
        return cases.get(id);
    }

    @PostMapping("/{id}/close")
    @PreAuthorize(Access.DECIDE)
    @Operation(summary = "Close a case, with a note saying why. 409 if already closed.")
    Cases.CaseDetail close(@PathVariable UUID id, @RequestBody DecisionRequest request, Authentication authentication) {
        return cases.close(id, Caller.of(authentication).name(), request.note());
    }
}
