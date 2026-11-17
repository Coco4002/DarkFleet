package com.darkfleet.intelligence.api;

import com.darkfleet.intelligence.decisions.Watchlist;
import com.darkfleet.intelligence.read.Views.WatchlistView;
import com.darkfleet.intelligence.web.PageResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/watchlist")
@PreAuthorize(Access.READ)
@Tag(name = "Watchlist")
class WatchlistController {

    private final Watchlist watchlist;

    WatchlistController(Watchlist watchlist) {
        this.watchlist = watchlist;
    }

    @GetMapping
    @Operation(summary = "Vessels on the watchlist; changed only by approving a WATCHLIST_CHANGE proposal")
    PageResult<WatchlistView> list(@RequestParam(defaultValue = "false") boolean includeRemoved,
                                   @RequestParam(defaultValue = "0") int page,
                                   @RequestParam(defaultValue = "" + PageResult.DEFAULT_SIZE) int size) {
        return watchlist.list(includeRemoved, page, size);
    }
}
