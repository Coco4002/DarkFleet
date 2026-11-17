package com.darkfleet.intelligence.api;

import com.darkfleet.intelligence.decisions.Assessments;
import com.darkfleet.intelligence.read.Views.AssessmentView;
import com.darkfleet.intelligence.web.Caller;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/assessments")
@Tag(name = "Assessments")
class AssessmentController {

    private final Assessments assessments;

    AssessmentController(Assessments assessments) {
        this.assessments = assessments;
    }

    @PostMapping
    @PreAuthorize(Access.PROPOSE)
    @Operation(summary = "Record that a detection needs no action, and why. 201 when recorded; 200 with the "
            + "existing verdict when the same requestId was sent before.")
    ResponseEntity<AssessmentView> record(@RequestBody Assessments.AssessmentRequest request,
                                          Authentication authentication) {
        Assessments.Recorded result = assessments.record(request, Caller.of(authentication).name());
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.assessment());
    }
}
