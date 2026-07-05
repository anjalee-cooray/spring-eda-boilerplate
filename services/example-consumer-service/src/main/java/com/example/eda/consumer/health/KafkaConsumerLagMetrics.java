package com.example.eda.consumer.health;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Exposes Kafka consumer group lag as Micrometer gauges.
 *
 * kafka.consumer.lag{group, topic, partition} reports how many messages
 * the consumer group is behind the latest offset on each partition.
 *
 * A lag of 0 means the consumer is caught up.
 * A growing lag indicates the consumer is processing slower than messages arrive.
 *
 * Alert rule (Prometheus):
 *   kafka_consumer_lag{group="example-consumer-service"} > 1000
 *   → consumer is more than 1000 messages behind — investigate processing rate
 *
 * This component only activates when app.events.broker=kafka.
 * For SNS/SQS, use the CloudWatch ApproximateNumberOfMessagesVisible metric instead.
 */
@Component
@ConditionalOnProperty(name = "app.events.broker", havingValue = "kafka")
public class KafkaConsumerLagMetrics {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerLagMetrics.class);

    private final AdminClient adminClient;
    private final MeterRegistry meterRegistry;

    // topic → partition → lag (refreshed every poll interval)
    private final Map<String, Map<Integer, Long>> lagSnapshot = new ConcurrentHashMap<>();

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${app.events.kafka.topics}")
    private String topics;

    @Value("${app.consumer.backpressure.lag-warn-threshold:5000}")
    private long lagWarnThreshold;

    public KafkaConsumerLagMetrics(AdminClient adminClient, MeterRegistry meterRegistry) {
        this.adminClient = adminClient;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void init() {
        // Gauges are registered once on startup and read from lagSnapshot on each scrape.
        // The snapshot is refreshed by refreshLag() on a schedule so Prometheus scrapes
        // are never blocked by a Kafka AdminClient network call.
        log.info("KafkaConsumerLagMetrics initialised for group={} topics={}", consumerGroupId, topics);
    }

    @Scheduled(fixedDelayString = "${app.kafka.lag-refresh-ms:15000}")
    public void refreshLag() {
        try {
            Set<TopicPartition> topicPartitions = resolveTopicPartitions();
            if (topicPartitions.isEmpty()) return;

            // Get current consumer group committed offsets
            Map<TopicPartition, Long> committed = adminClient
                    .listConsumerGroupOffsets(consumerGroupId)
                    .partitionsToOffsetAndMetadata()
                    .get()
                    .entrySet().stream()
                    .filter(e -> topicPartitions.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

            // Get latest offsets (end of each partition log)
            Map<TopicPartition, Long> endOffsets = adminClient
                    .listOffsets(topicPartitions.stream()
                            .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
                    .all()
                    .get()
                    .entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

            // lag = endOffset - committedOffset
            for (TopicPartition tp : topicPartitions) {
                long end       = endOffsets.getOrDefault(tp, 0L);
                long committed_ = committed.getOrDefault(tp, 0L);
                long lag       = Math.max(0, end - committed_);

                lagSnapshot
                        .computeIfAbsent(tp.topic(), t -> new ConcurrentHashMap<>())
                        .put(tp.partition(), lag);

                // Register gauge lazily on first observation of this partition
                String gaugeKey = tp.topic() + ":" + tp.partition();
                Gauge.builder("kafka.consumer.lag",
                        lagSnapshot, m -> m
                                .getOrDefault(tp.topic(), Map.of())
                                .getOrDefault(tp.partition(), 0L)
                                .doubleValue())
                        .tags("group", consumerGroupId, "topic", tp.topic(),
                              "partition", String.valueOf(tp.partition()))
                        .description("Kafka consumer group lag (messages behind latest)")
                        .register(meterRegistry);

                if (lag > lagWarnThreshold) {
                    log.warn("Kafka consumer lag high group={} topic={} partition={} lag={} threshold={}",
                            consumerGroupId, tp.topic(), tp.partition(), lag, lagWarnThreshold);
                } else {
                    log.debug("Kafka lag group={} topic={} partition={} lag={}",
                            consumerGroupId, tp.topic(), tp.partition(), lag);
                }
            }

        } catch (Exception ex) {
            log.warn("Failed to refresh Kafka consumer lag metrics", ex);
        }
    }

    /** Returns the maximum lag across all tracked partitions — 0 if no data yet. */
    public long getMaxLag() {
        return lagSnapshot.values().stream()
                .flatMap(partitionMap -> partitionMap.values().stream())
                .mapToLong(Long::longValue)
                .max()
                .orElse(0L);
    }

    private Set<TopicPartition> resolveTopicPartitions() {
        List<String> topicList = List.of(topics.split(","));
        try {
            return adminClient.describeTopics(topicList).allTopicNames().get()
                    .entrySet().stream()
                    .flatMap(e -> e.getValue().partitions().stream()
                            .map(p -> new TopicPartition(e.getKey(), p.partition())))
                    .collect(Collectors.toSet());
        } catch (Exception ex) {
            log.warn("Could not resolve topic partitions for lag metrics: {}", ex.getMessage());
            return Set.of();
        }
    }
}
