package com.example.eda.db;

import com.example.eda.events.envelope.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * JMH micro-benchmarks for the EventEnvelope serialization path.
 *
 * EventEnvelope serialization is in the hot path for every event the relay publishes:
 *   OutboxRelayPoller → publish() → KafkaEventPublisher.serialize() → ObjectMapper
 *
 * These benchmarks measure:
 *   1. Throughput of envelope serialization (ops/sec)
 *   2. Average latency per serialization (ns/op)
 *   3. Effect of payload size on serialization time
 *
 * How to run:
 *   ./gradlew :shared:shared-db:jmh
 *   Results: shared/shared-db/build/results/jmh/results.txt (human-readable)
 *            shared/shared-db/build/results/jmh/results.json (CI-parseable)
 *
 * Interpreting results:
 *   - A healthy envelope serialization should take < 10 µs (10,000 ns)
 *     for a typical payload (< 1 KB JSON)
 *   - If throughput drops below 50,000 ops/sec on a modern JVM, investigate:
 *     (a) ObjectMapper not reused (singleton vs. new instance per call)
 *     (b) Payload contains types that require expensive reflection
 *     (c) GC pressure from excessive object allocation
 *
 * Extending to OutboxWriter.write() benchmarks:
 *   OutboxWriter requires a Spring DataSource (JPA + PostgreSQL). To benchmark
 *   the full write path, set up an embedded H2 datasource or a Testcontainers
 *   PostgreSQL in the @Setup method and inject it into OutboxWriter manually.
 *   See OutboxWriterLoadTest for a functional throughput test using real Postgres.
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgs = {"--enable-preview"})
public class EventEnvelopeSerializationBenchmark {

    private ObjectMapper objectMapper;
    private EventEnvelope smallPayloadEnvelope;
    private EventEnvelope largePayloadEnvelope;
    private EventEnvelope minimalEnvelope;

    @Setup(Level.Trial)
    public void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules(); // registers JavaTimeModule for Instant

        // Small payload (~100 bytes of JSON)
        smallPayloadEnvelope = EventEnvelope.builder()
                .eventType("example.created")
                .tenantId("tenant-benchmark")
                .correlationId(UUID.randomUUID().toString())
                .schemaVersion("1")
                .occurredAt(Instant.now())
                .payload(Map.of("id", UUID.randomUUID().toString(), "name", "Benchmark Entity"))
                .build();

        // Large payload (~1 KB of JSON) — simulates a booking event with many fields
        smallPayloadEnvelope = EventEnvelope.builder()
                .eventType("example.created")
                .tenantId("tenant-benchmark")
                .correlationId(UUID.randomUUID().toString())
                .schemaVersion("2")
                .occurredAt(Instant.now())
                .aggregateId(UUID.randomUUID().toString())
                .payload(Map.of(
                        "id",          UUID.randomUUID().toString(),
                        "name",        "Benchmark Entity with Large Payload",
                        "description", "A".repeat(500),
                        "tags",        java.util.List.of("tag1", "tag2", "tag3", "tag4", "tag5"),
                        "metadata",    Map.of("key1", "value1", "key2", "value2", "key3", "value3"),
                        "createdAt",   Instant.now().toString(),
                        "updatedAt",   Instant.now().toString()
                ))
                .build();

        // Minimal envelope — baseline for the record + serialization overhead
        minimalEnvelope = EventEnvelope.builder()
                .eventType("ping")
                .tenantId("t")
                .correlationId("c")
                .schemaVersion("1")
                .occurredAt(Instant.now())
                .payload(Map.of("k", "v"))
                .build();
    }

    /**
     * Throughput benchmark: how many envelope serializations per microsecond?
     * This is the primary metric for relay throughput capacity.
     */
    @Benchmark
    public void serializeSmallPayload(Blackhole blackhole) throws Exception {
        blackhole.consume(objectMapper.writeValueAsString(smallPayloadEnvelope));
    }

    /**
     * Large payload: measures serialization scaling with payload size.
     * Expect this to be ~5–10x slower than the minimal envelope.
     */
    @Benchmark
    public void serializeLargePayload(Blackhole blackhole) throws Exception {
        blackhole.consume(objectMapper.writeValueAsString(largePayloadEnvelope));
    }

    /**
     * Minimal envelope: isolates the fixed overhead of ObjectMapper + record serialization
     * (Jackson reflection, Java record component access, Instant → ISO-8601 formatting).
     */
    @Benchmark
    public void serializeMinimalEnvelope(Blackhole blackhole) throws Exception {
        blackhole.consume(objectMapper.writeValueAsString(minimalEnvelope));
    }

    /**
     * Round-trip: serialize then deserialize back to EventEnvelope.
     * Measures the complete encoding cost per broker message (publish + consume path).
     */
    @Benchmark
    public void roundTripSmallPayload(Blackhole blackhole) throws Exception {
        String json = objectMapper.writeValueAsString(smallPayloadEnvelope);
        blackhole.consume(objectMapper.readValue(json, EventEnvelope.class));
    }
}
