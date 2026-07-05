package com.example.eda.query.rebuild;

/**
 * Request body for POST /projections/{name}/rebuild.
 *
 * targetVersion must be greater than the current version recorded in projection_versions.
 * requestedBy identifies the operator triggering the rebuild (username or system name).
 */
public record ReadModelRebuildRequest(
        int targetVersion,
        String requestedBy
) {
    public ReadModelRebuildRequest {
        if (targetVersion < 1) {
            throw new IllegalArgumentException("targetVersion must be >= 1");
        }
        if (requestedBy == null || requestedBy.isBlank()) {
            throw new IllegalArgumentException("requestedBy is required");
        }
    }
}
