package com.example.eda.db;

import com.example.eda.db.outbox.OutboxRecord;
import com.example.eda.db.outbox.OutboxRepository;
import com.example.eda.db.outbox.OutboxWriter;
import com.example.eda.security.TenantContext;
import com.example.eda.security.TenantContextHolder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxWriterTest {

    private final OutboxRepository repository = mock(OutboxRepository.class);
    private final OutboxWriter writer = new OutboxWriter(repository, new ObjectMapper());

    @BeforeEach
    void setTenantContext() {
        TenantContextHolder.set(new TenantContext("tenant-1", "user-1", List.of()));
    }

    @AfterEach
    void clearTenantContext() {
        TenantContextHolder.clear();
    }

    @Test
    void writesOutboxRecordWithCorrectFields() {
        ArgumentCaptor<OutboxRecord> captor = ArgumentCaptor.forClass(OutboxRecord.class);
        when(repository.save(captor.capture())).thenAnswer(inv -> inv.getArgument(0));

        writer.write("order.placed", new TestPayload("order-1"), "corr-123");

        OutboxRecord saved = captor.getValue();
        assertThat(saved.getTenantId()).isEqualTo("tenant-1");
        assertThat(saved.getEventType()).isEqualTo("order.placed");
        assertThat(saved.getCorrelationId()).isEqualTo("corr-123");
        assertThat(saved.getPayload()).contains("order-1");
        assertThat(saved.getStatus()).isEqualTo(OutboxRecord.OutboxStatus.PENDING);
        assertThat(saved.getEventId()).isNotNull();
    }

    record TestPayload(String orderId) { }
}
