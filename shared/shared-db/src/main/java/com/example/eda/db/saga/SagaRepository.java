package com.example.eda.db.saga;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaRepository extends JpaRepository<SagaInstance, UUID> {

    Optional<SagaInstance> findByCorrelationId(String correlationId);

    List<SagaInstance> findBySagaTypeAndStatus(String sagaType, SagaStatus status);

    /**
     * Returns STARTED or RUNNING sagas whose createdAt is before the cutoff timestamp.
     * Used by SagaTimeoutTask to detect stale sagas.
     */
    @org.springframework.data.jpa.repository.Query("""
            SELECT s FROM SagaInstance s
            WHERE s.status IN ('STARTED', 'RUNNING')
            AND   s.createdAt < :cutoff
            """)
    List<SagaInstance> findStartedBefore(@org.springframework.data.repository.query.Param("cutoff") java.time.Instant cutoff);
}
