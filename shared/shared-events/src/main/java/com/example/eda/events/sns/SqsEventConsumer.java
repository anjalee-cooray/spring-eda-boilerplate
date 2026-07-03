package com.example.eda.events.sns;

import com.example.eda.events.consumer.EventConsumer;
import com.example.eda.events.envelope.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "sns")
public class SqsEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SqsEventConsumer.class);

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final List<EventConsumer> consumers;
    private final SqsProperties sqsProperties;

    public SqsEventConsumer(
            SqsClient sqsClient,
            ObjectMapper objectMapper,
            List<EventConsumer> consumers,
            SqsProperties sqsProperties) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.consumers = consumers;
        this.sqsProperties = sqsProperties;
    }

    @Scheduled(fixedDelayString = "${app.events.sqs.poll-interval-ms:1000}")
    public void poll() {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(sqsProperties.queueUrl())
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                .build();

        List<Message> messages = sqsClient.receiveMessage(request).messages();

        for (Message message : messages) {
            try {
                EventEnvelope envelope = objectMapper.readValue(message.body(), EventEnvelope.class);
                consumers.stream()
                        .filter(c -> c.supports(envelope.eventType()))
                        .forEach(c -> c.handle(envelope));

                sqsClient.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(sqsProperties.queueUrl())
                        .receiptHandle(message.receiptHandle())
                        .build());
            } catch (Exception ex) {
                log.error("Failed to process SQS message messageId={}", message.messageId(), ex);
            }
        }
    }
}
