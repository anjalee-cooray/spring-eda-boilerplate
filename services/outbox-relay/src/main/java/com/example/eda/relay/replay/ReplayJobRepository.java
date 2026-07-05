package com.example.eda.relay.replay;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ReplayJobRepository extends JpaRepository<ReplayJob, UUID> {

    List<ReplayJob> findByTenantIdOrderByCreatedAtDesc(String tenantId);

    List<ReplayJob> findByStatusOrderByCreatedAtDesc(ReplayJob.ReplayStatus status);
}
