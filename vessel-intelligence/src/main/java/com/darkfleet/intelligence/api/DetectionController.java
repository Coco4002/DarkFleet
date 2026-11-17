package com.darkfleet.intelligence.api;

import com.darkfleet.common.model.DetectionStatus;
import com.darkfleet.common.model.DetectionType;
import com.darkfleet.intelligence.read.BoundingBox;
import com.darkfleet.intelligence.read.DetectionQueries;
import com.darkfleet.intelligence.read.Views.DetectionDetail;
import com.darkfleet.intelligence.read.Views.DetectionView;
import com.darkfleet.intelligence.web.PageResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@RequestMapping("/api/v1")
@PreAuthorize(Access.READ)
@Tag(name = "Detections")
class DetectionController {

    private final DetectionQueries detections;

    DetectionController(DetectionQueries detections) {
        this.detections = detections;
    }

    @GetMapping("/detections")
    @Operation(summary = "Detections that started in [from, to), newest first; bbox is minLon,minLat,maxLon,maxLat")
    PageResult<DetectionView> list(@RequestParam(required = false) DetectionType type,
                                   @RequestParam(required = false) DetectionStatus status,
                                   @RequestParam(required = false) String mmsi,
                                   @RequestParam(required = false) Instant from,
                                   @RequestParam(required = false) Instant to,
                                   @RequestParam(required = false) String bbox,
                                   @RequestParam(defaultValue = "0") int page,
                                   @RequestParam(defaultValue = "" + PageResult.DEFAULT_SIZE) int size) {
        return detections.list(type, status, mmsi, from, to, BoundingBox.parse(bbox), page, size);
    }

    @GetMapping("/detections/{id}")
    @Operation(summary = "One detection with its evidence, recorded assessments and the cases it belongs to")
    DetectionDetail get(@PathVariable String id) {
        return detections.get(id);
    }

    @GetMapping("/encounters")
    @Operation(summary = "Potential encounters that started in [from, to); bbox is minLon,minLat,maxLon,maxLat")
    PageResult<DetectionView> encounters(@RequestParam(required = false) DetectionStatus status,
                                         @RequestParam(required = false) String mmsi,
                                         @RequestParam(required = false) Instant from,
                                         @RequestParam(required = false) Instant to,
                                         @RequestParam(required = false) String bbox,
                                         @RequestParam(defaultValue = "0") int page,
                                         @RequestParam(defaultValue = "" + PageResult.DEFAULT_SIZE) int size) {
        return detections.list(DetectionType.POTENTIAL_ENCOUNTER, status, mmsi, from, to, BoundingBox.parse(bbox),
                page, size);
    }
}
