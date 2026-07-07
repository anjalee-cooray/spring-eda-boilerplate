package com.example.eda.consumer.health;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for KafkaConsumerLagMetrics against a real Kafka broker.
 *
 * Unit tests verify the state machine of KafkaBackpressureController (mock lag).
 * These integration tests verify the lag CALCULATION itself — that getMaxLag()
 * correctly reads committed offsets vs. end offsets from a real Kafka cluster.
 *
 * Test scenarios:
 *
 *   1. No unconsumed messages (consumer group caught up) → lag = 0
 *   2. 100 messages published, consumer group committed at offset 60 → lag = 40
 *   3. 100 messages published, no consumer group committed offsets → lag = 100
 *
 * The test controls committed offsets via AdminClient.alterConsumerGroupOffsets()
 * rather than running a real consumer, making the tests fast and deterministic.
 *
 * Why this matters:
 *   KafkaConsumerLagMetrics reads from AdminClient (not the consumer's own metrics).
 *   If the AdminClient API behaviour changes, or if the lag calculation logic has a
 *   sign error, only an integration test against a real broker will catch it.
 */
@Testcontainers
@Tag("integration")
class KafkaConsumerLagIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    private static final String TOPIC         = "lag-test-topic";
    private static final String CONSUMER_GROUP = "lag-test-group";

    private AdminClient adminClient;
    private KafkaProducer<String, String> producer;
    private KafkaConsumerLagMetrics lagMetrics;

    @BeforeEach
    void setUp() throws Exception {
        Map<String, Object> adminConfig = Map.of(
                "bootstrap.servers", kafka.getBootstrapServers());

        adminClient = AdminClient.create(adminConfig);

        // Create topic (single partition for deterministic offset arithmetic)
        adminClient.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();

        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        lagMetrics = new KafkaConsumerLagMetrics(adminClient, meterRegistry);

        // Set @Value fields via reflection (not in Spring context)
        setField(lagMetrics, "consumerGroupId", CONSUMER_GROUP);
        setField(lagMetrics, "topics", TOPIC);
        setField(lagMetrics, "lagWarnThreshold", 50L);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (producer != null) producer.close();
        if (adminClient != null) {
            adminClient.deleteTopics(List.of(TOPIC)).all().get();
            adminClient.close();
        }
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * When the consumer group has consumed all messages (committed offset = end offset),
     * getMaxLag() must return 0.
     */
    @Test
    void getMaxLag_consumerCaughtUp_returnsZero() throws Exception {
        int messageCount = 20;
        publishMessages(messageCount);

        // Commit offset at the end of the partition (consumer fully caught up)
        commitOffset(messageCount);

        lagMetrics.refreshLag();

        assertThat(lagMetrics.getMaxLag())
                .as("Consumer fully caught up — lag must be 0")
                .isZero();
    }

    /**
     * When 100 messages are published and the consumer group has committed at offset 60,
     * getMaxLag() must return 40 (the unconsumed tail).
     */
    @Test
    void getMaxLag_partialConsumption_returnsCorrectLag() throws Exception {
        int published  = 100;
        int consumed   = 60;
        int expectedLag = published - consumed;

        publishMessages(published);
        commitOffset(consumed);

        lagMetrics.refreshLag();

        assertThat(lagMetrics.getMaxLag())
                .as("Expected lag=%d (published=%d, consumed=%d)", expectedLag, published, consumed)
                .isEqualTo(expectedLag);
    }

    /**
     * When messages are published but the consumer group has no committed offsets yet
     * (fresh group, never consumed), lag equals the total number of messages.
     */
    @Test
    void getMaxLag_freshConsumerGroup_lagEqualsMessageCount() throws Exception {
        int messageCount = 50;
        publishMessages(messageCount);

        // No commitOffset() call — consumer group has never committed
        lagMetrics.refreshLag();

        // Fresh consumer group: committed offset = 0, end offset = 50 → lag = 50
        assertThat(lagMetrics.getMaxLag())
                .as("Fresh consumer group with no commits — lag must equal message count")
                .isEqualTo(messageCount);
    }

    /**
     * With an empty topic and no messages, lag must be 0 regardless of group state.
     */
    @Test
    void getMaxLag_emptyTopic_returnsZero() throws Exception {
        // No messages published; topic exists but is empty
        lagMetrics.refreshLag();

        assertThat(lagMetrics.getMaxLag())
                .as("Empty topic — no lag possible")
                .isZero();
    }

    /**
     * Verifies that backpressure controller transitions to PAUSED when lag from a real
     * KafkaConsumerLagMetrics instance (backed by a real Kafka cluster) exceeds the threshold.
     *
     * This end-to-end test proves the wiring from real lag calculation to controller action.
     */
    @Test
    void backpressureController_realLag_pausesContainers() throws Exception {
        int messageCount = 200;
        publishMessages(messageCount); // consumer group has 0 committed offset → lag = 200

        lagMetrics.refreshLag();
        assertThat(lagMetrics.getMaxLag()).isEqualTo(messageCount);

        // Wire real lag metrics into the controller (mock only the registry for simplicity)
        io.micrometer.core.instrument.simple.SimpleMeterRegistry meterRegistry
                = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();

        org.springframework.kafka.listener.MessageListenerContainer container
                = org.mockito.Mockito.mock(org.springframework.kafka.listener.MessageListenerContainer.class);

        org.springframework.kafka.config.KafkaListenerEndpointRegistry registry
                = org.mockito.Mockito.mock(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class);
        org.mockito.Mockito.when(registry.getListenerContainers())
                .thenReturn((java.util.Collection) List.of(container));

        KafkaBackpressureController controller
                = new KafkaBackpressureController(registry, lagMetrics, meterRegistry);
        setField(controller, "lagPauseThreshold", 100L);
        setField(controller, "lagResumeThreshold", 10L);

        controller.checkAndAdjust();

        // Verify container.pause() was called because real lag (200) > threshold (100)
        org.mockito.Mockito.verify(container).pause();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void publishMessages(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            producer.send(new ProducerRecord<>(TOPIC, "key-" + i, "value-" + i)).get();
        }
    }

    private void commitOffset(long offset) throws Exception {
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        adminClient.alterConsumerGroupOffsets(
                CONSUMER_GROUP,
                Map.of(tp, new OffsetAndMetadata(offset))).all().get();
    }

    private static void setField(Object target, String name, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field " + name, e);
        }
    }
}
