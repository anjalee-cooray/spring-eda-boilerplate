package com.example.eda.db.inbox;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InboxRepository extends JpaRepository<InboxRecord, UUID> {

    boolean existsByEventId(UUID eventId);
}
