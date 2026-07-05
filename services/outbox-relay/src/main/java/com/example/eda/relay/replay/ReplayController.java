package com.example.eda.relay.replay;

import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/**
 * Operator API for triggering and monitoring event replay jobs.
 *
 * This endpoint is internal — expose it only on an operator network, not through
 * the public api-gateway. In production, restrict access via VPC security group
 * or mTLS so only authorised operator tooling can trigger replays.
 *
 * API:
 *
 *   POST /replay/jobs               — trigger a new replay job (returns 202 Accepted)
 *   GET  /replay/jobs/{id}          — poll a job's status and progress
 *   GET  /replay/jobs?tenantId=...  — list all jobs for a tenant
 *
 * Example — replay all events for a tenant (full read model rebuild):
 *   curl -X POST http://outbox-relay:8084/replay/jobs \
 *     -H "Content-Type: application/json" \
 *     -d '{"tenantId":"tenant-1","requestedBy":"ops-team"}'
 *
 * Example — replay a specific event type after a bug fix:
 *   curl -X POST http://outbox-relay:8084/replay/jobs \
 *     -H "Content-Type: application/json" \
 *     -d '{"tenantId":"tenant-1","eventType":"example.created","requestedBy":"ops"}'
 *
 * Example — replay within a date range (outage recovery):
 *   curl -X POST http://outbox-relay:8084/replay/jobs \
 *     -H "Content-Type: application/json" \
 *     -d '{"tenantId":"tenant-1","fromTimestamp":"2025-01-01T00:00:00Z",
 *           "toTimestamp":"2025-01-02T00:00:00Z","requestedBy":"ops"}'
 *
 * Example — replay specific records by outbox ID:
 *   curl -X POST http://outbox-relay:8084/replay/jobs \
 *     -H "Content-Type: application/json" \
 *     -d '{"tenantId":"tenant-1","specificIds":["uuid1","uuid2"],"requestedBy":"ops"}'
 *
 * Poll progress:
 *   curl http://outbox-relay:8084/replay/jobs/{jobId}
 */
@RestController
@RequestMapping("/replay/jobs")
@PreAuthorize("hasRole('PLATFORM_OPERATOR')")
public class ReplayController {

    private static final Logger log = LoggerFactory.getLogger(ReplayController.class);

    private final ReplayJobService replayJobService;
    private final ReplayJobRepository replayJobRepository;

    public ReplayController(ReplayJobService replayJobService, ReplayJobRepository replayJobRepository) {
        this.replayJobService   = replayJobService;
        this.replayJobRepository = replayJobRepository;
    }

    /**
     * Trigger a replay job. Returns 202 Accepted immediately.
     * Replay runs asynchronously — poll GET /replay/jobs/{id} for progress.
     */
    @PostMapping
    public ResponseEntity<ReplayJobResponse> create(@RequestBody ReplayRequest request) {
        ReplayJob job = replayJobService.create(request);

        // Kick off async execution — returns immediately, job runs in background
        replayJobService.execute(job.getId());

        var location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(job.getId())
                .toUri();

        log.info("Replay job accepted id={} tenantId={} requestedBy={}",
                job.getId(), job.getTenantId(), job.getRequestedBy());

        return ResponseEntity.accepted()
                .location(location)
                .body(ReplayJobResponse.from(job));
    }

    /**
     * Get a single job's current status and progress.
     */
    @GetMapping("/{id}")
    public ResponseEntity<ReplayJobResponse> get(@PathVariable UUID id) {
        return replayJobRepository.findById(id)
                .map(job -> ResponseEntity.ok(ReplayJobResponse.from(job)))
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * List all replay jobs, optionally filtered by tenantId.
     */
    @GetMapping
    public List<ReplayJobResponse> list(@RequestParam(required = false) String tenantId) {
        List<ReplayJob> jobs = tenantId != null
                ? replayJobRepository.findByTenantIdOrderByCreatedAtDesc(tenantId)
                : replayJobRepository.findAll();

        return jobs.stream().map(ReplayJobResponse::from).toList();
    }
}
