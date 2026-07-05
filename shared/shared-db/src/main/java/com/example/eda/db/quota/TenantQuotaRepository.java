package com.example.eda.db.quota;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TenantQuotaRepository extends JpaRepository<TenantQuota, UUID> {

    Optional<TenantQuota> findByTenantId(String tenantId);
}
