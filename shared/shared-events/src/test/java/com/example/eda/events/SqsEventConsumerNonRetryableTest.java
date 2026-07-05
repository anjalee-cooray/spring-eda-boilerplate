package com.example.eda.events;

import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import com.example.eda.events.sns.SqsBackpressureController;
import com.example.eda.events.sns.SqsEventConsumer;
import com.example.eda.events.sns.SqsProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqsEventConsumerNonRetryableTest {

    private final SqsClient sqsClient = mock(SqsClient.class);
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final SqsProperties props = new SqsProperties(
            "https://sqs.us-east-1.amazonaws.com/123/main-queue",
            "https://sqs.us-east-1.amazonaws.com/123/dlq",
            1000, "us-east-1", null);

    private EventConsumer happyConsumer;
    private SqsEventConsumer consumer;

    @BeforeEach
    void setUp() {
        happyConsumer = mock(EventConsumer.class);
        when(happyConsumer.supports(any())).thenReturn(true);
        consumer = new SqsEventConsumer(sqsClient, objectMapper, List.of(happyConsumer),
                props, Optional.empty(), Optional.empty());
    }

    private Message messageWithBody(String body) {
        return Message.builder()
                .messageId("msg-001")
                .receiptHandle("receipt-001")
                .body(body)
                .attributesWithStrings(Map.of(
                        MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT.toString(), "1"))
                .build();
    }

    private String validEnvelopeJson(String eventType) throws Exception {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventType(eventType)
                .tenantId("tenant-1")
                .correlationId("corr-1")
                .payload(Map.of("key", "value"))
                .build();
        return objectMapper.writeValueAsString(envelope);
    }

    @Test
    void nonRetryableExceptionRoutesToDlqAndDeletesFromSource() throws Exception {
        String body = validEnvelopeJson("example.created");
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messageWithBody(body)).build());

        // Consumer throws a non-retryable exception
        org.mockito.Mockito.doThrow(new IllegalArgumentException("bad payload"))
                .when(happyConsumer).handle(any());

        consumer.poll();

        // Should write to DLQ
        ArgumentCaptor<SendMessageRequest> sendCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient).sendMessage(sendCaptor.capture());
        assertThat(sendCaptor.getValue().queueUrl()).isEqualTo(props.dlqQueueUrl());

        // Should delete from source
        ArgumentCaptor<DeleteMessageRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteMessageRequest.class);
        verify(sqsClient).deleteMessage(deleteCaptor.capture());
        assertThat(deleteCaptor.getValue().queueUrl()).isEqualTo(props.queueUrl());
    }

    @Test
    void retryableExceptionExtendsVisibilityNotDlq() throws Exception {
        String body = validEnvelopeJson("example.created");
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messageWithBody(body)).build());

        // Consumer throws a retryable exception (not in NON_RETRYABLE set)
        org.mockito.Mockito.doThrow(new RuntimeException("transient"))
                .when(happyConsumer).handle(any());

        consumer.poll();

        // Should NOT write to DLQ
        verify(sqsClient, never()).sendMessage(any(SendMessageRequest.class));

        // Should extend visibility timeout for retry
        verify(sqsClient).changeMessageVisibility(any());
    }

    @Test
    void successfulProcessingDeletesFromSource() throws Exception {
        String body = validEnvelopeJson("example.created");
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messageWithBody(body)).build());

        consumer.poll();

        verify(sqsClient).deleteMessage(any(DeleteMessageRequest.class));
        verify(sqsClient, never()).sendMessage(any(SendMessageRequest.class));
    }
}
