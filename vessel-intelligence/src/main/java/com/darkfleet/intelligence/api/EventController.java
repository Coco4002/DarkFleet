package com.darkfleet.intelligence.api;

import com.darkfleet.intelligence.events.AnalystEvents;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/v1/events")
@PreAuthorize(Access.DECIDE)
@Tag(name = "Live events")
class EventController {

    private final AnalystEvents events;

    EventController(AnalystEvents events) {
        this.events = events;
    }

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Server-sent events for analysts: 'detection' when one opens or closes, 'proposal' when one "
            + "is created or decided; a heartbeat comment every 20 s")
    SseEmitter stream() {
        return events.connect();
    }
}
