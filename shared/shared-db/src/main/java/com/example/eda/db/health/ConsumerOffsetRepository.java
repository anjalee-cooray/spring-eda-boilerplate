package com.example.eda.db.health;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ConsumerOffsetRepository extends JpaRepository<ConsumerOffset, ConsumerOffset.ConsumerOffsetId> {

    List<ConsumerOffset> findByConsumerName(String consumerName);

    List<ConsumerOffset> findAllByOrderByLastEventAtAsc();

    Optional<ConsumerOffset> findByConsumerNameAndEventTypeAndTenantId(
            String consumerName, String eventType, String tenantId);
}
