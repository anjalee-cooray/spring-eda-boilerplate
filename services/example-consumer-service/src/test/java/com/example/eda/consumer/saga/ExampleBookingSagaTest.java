package com.example.eda.consumer.saga;

import com.example.eda.db.outbox.OutboxRecord;
import com.example.eda.db.outbox.OutboxWriter;
import com.example.eda.db.saga.SagaInstance;
import com.example.eda.db.saga.SagaRepository;
import com.example.eda.db.saga.SagaStatus;
import com.example.eda.events.envelope.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExampleBookingSagaTest {

    private final SagaRepository sagaRepository = mock(SagaRepository.class);
    private final OutboxWriter outboxWriter = mock(OutboxWriter.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ExampleBookingSaga saga = new ExampleBookingSaga(sagaRepository, outboxWriter, objectMapper);

    private static final String CORRELATION_ID = "corr-booking-001";

    private EventEnvelope bookingRequestedEnvelope() {
        Map<String, Object> payload = Map.of(
                "bookingId", "booking-1",
                "customerId", "cust-1",
                "itemId", "item-1",
                "amount", "99.00"
        );
        return EventEnvelope.builder()
                .eventType("booking.requested")
                .tenantId("tenant-1")
                .correlationId(CORRELATION_ID)
                .payload(payload)
                .build();
    }

    private EventEnvelope envelopeFor(String eventType) {
        return EventEnvelope.builder()
                .eventType(eventType)
                .tenantId("tenant-1")
                .correlationId(CORRELATION_ID)
                .payload(Map.of())
                .build();
    }

    private SagaInstance runningInstance(String step) {
        return SagaInstance.builder()
                .sagaType(ExampleBookingSaga.SAGA_TYPE)
                .correlationId(CORRELATION_ID)
                .currentStep(step)
                .context("{\"bookingId\":\"booking-1\",\"customerId\":\"cust-1\","
                        + "\"itemId\":\"item-1\",\"amount\":\"99.00\",\"tenantId\":\"tenant-1\"}")
                .build();
    }

    @BeforeEach
    void stubOutboxWriter() {
        when(outboxWriter.write(anyString(), any(), anyString()))
                .thenReturn(mock(OutboxRecord.class));
    }

    // ── booking.requested — starts saga and writes inventory reserve command ─

    @Test
    void startCreatesSagaAndWritesInventoryReserveCommand() {
        when(sagaRepository.findByCorrelationId(CORRELATION_ID)).thenReturn(Optional.empty());
        when(sagaRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        saga.handle(bookingRequestedEnvelope());

        ArgumentCaptor<SagaInstance> captor = ArgumentCaptor.forClass(SagaInstance.class);
        verify(sagaRepository).save(captor.capture());
        assertThat(captor.getValue().getCurrentStep()).isEqualTo(ExampleBookingSaga.STEP_AWAITING_INVENTORY);

        verify(outboxWriter).write(eq(ExampleBookingSaga.CMD_INVENTORY_RESERVE), any(), eq(CORRELATION_ID));
    }

    @Test
    void duplicateBookingRequestIgnored() {
        when(sagaRepository.findByCorrelationId(CORRELATION_ID))
                .thenReturn(Optional.of(runningInstance(ExampleBookingSaga.STEP_AWAITING_INVENTORY)));

        saga.handle(bookingRequestedEnvelope());

        verify(sagaRepository, never()).save(any());
        verify(outboxWriter, never()).write(anyString(), any(), anyString());
    }

    // ── booking.inventory.confirmed — advances to AWAITING_PAYMENT ───────────

    @Test
    void inventoryConfirmedAdvancesToPaymentStep() {
        SagaInstance instance = runningInstance(ExampleBookingSaga.STEP_AWAITING_INVENTORY);
        when(sagaRepository.findByCorrelationId(CORRELATION_ID)).thenReturn(Optional.of(instance));
        when(sagaRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        saga.handle(envelopeFor("booking.inventory.confirmed"));

        assertThat(instance.getCurrentStep()).isEqualTo(ExampleBookingSaga.STEP_AWAITING_PAYMENT);
        verify(outboxWriter).write(eq(ExampleBookingSaga.CMD_PAYMENT_CHARGE), any(), eq(CORRELATION_ID));
    }

    // ── booking.payment.confirmed — completes the saga ────────────────────────

    @Test
    void paymentConfirmedCompletesSaga() {
        SagaInstance instance = runningInstance(ExampleBookingSaga.STEP_AWAITING_PAYMENT);
        when(sagaRepository.findByCorrelationId(CORRELATION_ID)).thenReturn(Optional.of(instance));
        when(sagaRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        saga.handle(envelopeFor("booking.payment.confirmed"));

        assertThat(instance.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        verify(outboxWriter, never()).write(anyString(), any(), anyString());
    }

    // ── booking.payment.failed — starts compensation ───────────────────────────

    @Test
    void paymentFailedStartsCompensation() {
        SagaInstance instance = runningInstance(ExampleBookingSaga.STEP_AWAITING_PAYMENT);
        when(sagaRepository.findByCorrelationId(CORRELATION_ID)).thenReturn(Optional.of(instance));
        when(sagaRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        saga.handle(envelopeFor("booking.payment.failed"));

        assertThat(instance.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        verify(outboxWriter).write(eq(ExampleBookingSaga.CMD_INVENTORY_RELEASE), any(), eq(CORRELATION_ID));
    }

    // ── booking.inventory.released — saga is COMPENSATED ────────────────────

    @Test
    void inventoryReleasedMarksCompensated() {
        SagaInstance instance = runningInstance(ExampleBookingSaga.COMP_AWAITING_INVENTORY_RELEASE);
        instance.startCompensation("payment failed");
        instance.advanceTo(ExampleBookingSaga.COMP_AWAITING_INVENTORY_RELEASE);
        when(sagaRepository.findByCorrelationId(CORRELATION_ID)).thenReturn(Optional.of(instance));
        when(sagaRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        saga.handle(envelopeFor("booking.inventory.released"));

        assertThat(instance.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }
}
