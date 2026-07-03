package com.example.eda.db;

import com.example.eda.db.inbox.InboxDeduplicator;
import com.example.eda.db.inbox.InboxRecord;
import com.example.eda.db.inbox.InboxRepository;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InboxDeduplicatorTest {

    private final InboxRepository repository = mock(InboxRepository.class);
    private final InboxDeduplicator deduplicator = new InboxDeduplicator(repository);

    @Test
    void returnsTrueWhenEventAlreadyProcessed() {
        UUID eventId = UUID.randomUUID();
        when(repository.existsByEventId(eventId)).thenReturn(true);

        assertThat(deduplicator.isDuplicate(eventId)).isTrue();
    }

    @Test
    void returnsFalseWhenEventNotYetProcessed() {
        UUID eventId = UUID.randomUUID();
        when(repository.existsByEventId(eventId)).thenReturn(false);

        assertThat(deduplicator.isDuplicate(eventId)).isFalse();
    }

    @Test
    void markProcessedSavesInboxRecord() {
        UUID eventId = UUID.randomUUID();
        ArgumentCaptor<InboxRecord> captor = ArgumentCaptor.forClass(InboxRecord.class);
        when(repository.save(captor.capture())).thenAnswer(inv -> inv.getArgument(0));

        deduplicator.markProcessed(eventId, "order.placed", "tenant-1");

        InboxRecord saved = captor.getValue();
        assertThat(saved.getEventId()).isEqualTo(eventId);
        assertThat(saved.getEventType()).isEqualTo("order.placed");
        assertThat(saved.getTenantId()).isEqualTo("tenant-1");
        assertThat(saved.getProcessedAt()).isNotNull();
    }
}
