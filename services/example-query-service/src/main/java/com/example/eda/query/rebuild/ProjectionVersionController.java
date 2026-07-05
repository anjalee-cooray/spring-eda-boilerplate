package com.example.eda.query.rebuild;

import com.example.eda.db.projection.ProjectionVersionRecord;
import com.example.eda.db.projection.ProjectionVersionRepository;
import com.example.eda.security.TenantContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Manages CQRS read-model projection versions and rebuilds.
 *
 * Rebuild flow:
 *   1. POST /projections/{name}/rebuild  — initiates async rebuild, returns immediately
 *   2. GET  /projections/{name}/version  — poll status (CURRENT | REBUILDING | FAILED)
 *   3. GET  /projections               — list all projections with their versions
 *
 * Only PLATFORM_OPERATOR may trigger rebuilds — this is a destructive, long-running op.
 */
@RestController
@RequestMapping("/projections")
public class ProjectionVersionController {

    private static final Logger log = LoggerFactory.getLogger(ProjectionVersionController.class);

    private final ProjectionVersionRepository projectionVersionRepository;
    private final ReadModelRebuildService rebuildService;

    public ProjectionVersionController(
            ProjectionVersionRepository projectionVersionRepository,
            ReadModelRebuildService rebuildService) {
        this.projectionVersionRepository = projectionVersionRepository;
        this.rebuildService = rebuildService;
    }

    @GetMapping
    @PreAuthorize("hasRole('PLATFORM_OPERATOR')")
    public List<ProjectionVersionRecord> listAll() {
        return projectionVersionRepository.findAll();
    }

    @GetMapping("/{name}/version")
    @PreAuthorize("hasRole('PLATFORM_OPERATOR')")
    public ResponseEntity<ProjectionVersionRecord> getVersion(@PathVariable String name) {
        return projectionVersionRepository.findByProjectionName(name)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{name}/rebuild")
    @PreAuthorize("hasRole('PLATFORM_OPERATOR')")
    public ResponseEntity<ReadModelRebuildResponse> rebuild(
            @PathVariable String name,
            @RequestBody ReadModelRebuildRequest request) {
        String tenantId = TenantContextHolder.get().tenantId();

        log.info("Rebuild requested projection={} tenant={} targetVersion={} requestedBy={}",
                name, tenantId, request.targetVersion(), request.requestedBy());

        ReadModelRebuildResponse response = rebuildService.initiate(name, tenantId, request);
        return ResponseEntity.accepted().body(response);
    }
}
