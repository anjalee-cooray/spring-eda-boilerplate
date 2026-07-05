package com.example.eda.db.saga;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaRepository extends JpaRepository<SagaInstance, UUID> {

    Optional<SagaInstance> findByCorrelationId(String correlationId);

    List<SagaInstance> findBySagaTypeAndStatus(String sagaType, SagaStatus status);
}
