package com.example.eda.query.rebuild;

import java.util.UUID;

/**
 * Returned by POST /projections/{name}/rebuild.
 * The rebuild runs asynchronously — poll the replayJobId via the outbox-relay
 * GET /replay/jobs/{replayJobId} endpoint to check progress.
 */
public record ReadModelRebuildResponse(
        String projectionName,
        int targetVersion,
        UUID replayJobId,
        String status,
        String message
) {}
