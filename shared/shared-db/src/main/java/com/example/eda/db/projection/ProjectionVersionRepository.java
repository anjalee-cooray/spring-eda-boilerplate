package com.example.eda.db.projection;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProjectionVersionRepository extends JpaRepository<ProjectionVersionRecord, UUID> {

    Optional<ProjectionVersionRecord> findByProjectionName(String projectionName);
}
