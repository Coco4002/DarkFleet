package com.darkfleet.intelligence.api;

import com.darkfleet.intelligence.decisions.RuleSets;
import com.darkfleet.intelligence.web.PageResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/rules")
@PreAuthorize(Access.READ)
@Tag(name = "Detection rules")
class RuleController {

    private final RuleSets rules;

    RuleController(RuleSets rules) {
        this.rules = rules;
    }

    @GetMapping("/current")
    @Operation(summary = "The rule set in force (version 0 is the baseline)")
    RuleSets.RuleSetView current() {
        return rules.current();
    }

    @GetMapping
    @Operation(summary = "Every rule set, newest first; changed only by approving a RULE_CHANGE proposal")
    PageResult<RuleSets.RuleSetView> history(@RequestParam(defaultValue = "0") int page,
                                             @RequestParam(defaultValue = "" + PageResult.DEFAULT_SIZE) int size) {
        return rules.history(page, size);
    }
}
