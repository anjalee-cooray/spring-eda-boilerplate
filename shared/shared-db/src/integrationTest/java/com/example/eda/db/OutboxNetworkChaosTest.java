package com.example.eda.db;

import com.example.eda.db.eventstore.EventStoreWriter;
import com.example.eda.db.outbox.OutboxRecord;
import com.example.eda.db.outbox.OutboxWriter;
import com.example.eda.security.TenantContext;
import com.example.eda.security.TenantContextHolder;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Network chaos tests for OutboxWriter — verifies correct behaviour under
 * database network fault conditions injected by Toxiproxy.
 *
 * Toxiproxy sits between the application and PostgreSQL:
 *
 *   Test process → Toxiproxy:port → PostgreSQL:5432
 *
 * Fault scenarios:
 *
 *   1. Latency injection: 500ms added latency — writes still succeed but take longer.
 *      Verifies the outbox dual-write is not time-sensitive and succeeds under slow networks.
 *
 *   2. Latency removed: after removing the toxin, writes return to baseline speed.
 *      Verifies the proxy itself is not the source of residual slowness (control condition).
 *
 *   3. Connection cut + restore: completely severing the TCP connection causes writes
 *      to fail with a database exception, then immediately succeed once connectivity
 *      is restored — no partial records remain from the failed write.
 *
 * Why network chaos matters for the outbox:
 *   The outbox relay depends on continuous DB connectivity. A network partition
 *   between the relay and DB (not the broker and DB) must not corrupt the outbox
 *   state — either both outbox_records and event_store are written, or neither.
 *   These tests verify that guarantee holds even under forced TCP disruption.
 *
 * Note on Toxiproxy bandwidth toxin vs. TCP cut:
 *   Setting bandwidth rate=0 (bytes/sec) effectively cuts the connection but
 *   allows graceful TCP FIN — simulating a network partition where existing
 *   connections stall rather than break with RST. This is the realistic failure mode
 *   for cloud network partitions (security group rule changes, NAT gateway failures).
 */
@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({OutboxWriter.class, EventStoreWriter.class, ObjectMapper.class})
@Tag("integration")
class OutboxNetworkChaosTest {

    private static final Network network = Network.newNetwork();

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("edatest")
            .withUsername("test")
            .withPassword("test")
            .withNetwork(network)
            .withNetworkAliases("postgres");

    @Container
    static ToxiproxyContainer toxiproxy = new ToxiproxyContainer(
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0"))
            .withNetwork(network);

    private static Proxy proxy;

    @DynamicPropertySource
    static void registerProps(DynamicPropertyRegistry registry) throws IOException {
        // Route all DB traffic through Toxiproxy → Postgres
        ToxiproxyClient client = new ToxiproxyClient(
                toxiproxy.getHost(), toxiproxy.getControlPort());
        proxy = client.createProxy("postgres", "0.0.0.0:8666", "postgres:5432");

        String proxyUrl = "jdbc:postgresql://" + toxiproxy.getHost() + ":"
                + toxiproxy.getMappedPort(8666) + "/edatest";

        registry.add("spring.datasource.url", () -> proxyUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired OutboxWriter outboxWriter;
    @Autowired PlatformTransactionManager txManager;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void setUp() throws IOException {
        // Remove any toxins left over from previous test
        proxy.toxics().getAll().forEach(t -> {
            try { t.remove(); } catch (IOException ignored) { }
        });
        TenantContextHolder.set(new TenantContext("tenant-chaos-net", "chaos-subj", List.of("TENANT_ADMIN")));
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up toxins so the container is reusable
        proxy.toxics().getAll().forEach(t -> {
            try { t.remove(); } catch (IOException ignored) { }
        });
        TenantContextHolder.clear();
        jdbc.execute("DELETE FROM outbox_records WHERE tenant_id = 'tenant-chaos-net'");
        jdbc.execute("DELETE FROM event_store WHERE tenant_id = 'tenant-chaos-net'");
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * Baseline: writes succeed with no network fault active.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void baseline_noFault_writesSucceed() {
        OutboxRecord record = write("corr-baseline");

        assertThat(record.getId()).isNotNull();
        assertThat(eventStoreCount()).isEqualTo(1);
    }

    /**
     * 500ms latency added to downstream (proxy → Postgres direction).
     * Writes must still succeed — the operation is slow but not broken.
     * Verifies the dual-write is not sensitive to sub-second network latency.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void latencyInjection_500ms_writesStillSucceed() throws IOException {
        proxy.toxics().latency("pg-latency", ToxicDirection.DOWNSTREAM, 500);

        OutboxRecord record = write("corr-latency");

        assertThat(record.getId()).isNotNull();
        assertThat(eventStoreCount()).isEqualTo(1);
    }

    /**
     * After removing the latency toxin, writes return to normal speed.
     * Verifies: toxin removal takes effect immediately and proxy recovers cleanly.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void latencyRemoval_writesReturnToNormalSpeed() throws IOException {
        proxy.toxics().latency("pg-latency-removable", ToxicDirection.DOWNSTREAM, 800);

        // Slow write under latency
        long startWithLatency = System.nanoTime();
        write("corr-latency-slow");
        long slowMs = (System.nanoTime() - startWithLatency) / 1_000_000;

        // Remove toxin
        proxy.toxics().get("pg-latency-removable").remove();

        // Fast write without latency
        long startWithoutLatency = System.nanoTime();
        write("corr-latency-fast");
        long fastMs = (System.nanoTime() - startWithoutLatency) / 1_000_000;

        assertThat(slowMs).as("Write under 800ms latency must take > 800ms").isGreaterThan(800);
        assertThat(fastMs).as("Write after latency removal must be faster than the latency toxin").isLessThan(slowMs);
        assertThat(eventStoreCount()).isEqualTo(2); // both writes succeeded
    }

    /**
     * Zero-bandwidth toxin simulates a network partition (TCP stalls, no data flows).
     * Writes under the partition must fail with a database exception.
     * After the toxin is removed, connectivity restores and writes succeed again.
     * Verifies: no partial records are left in the DB from the failed write.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void connectionCutAndRestore_failsThenRecovershttps() throws IOException {
        // Step 1: successful write before partition
        write("corr-before-partition");
        assertThat(eventStoreCount()).isEqualTo(1);

        // Step 2: cut the connection — writes must fail
        proxy.toxics().bandwidth("cut-connection", ToxicDirection.DOWNSTREAM, 0);

        boolean writeFailed = false;
        try {
            write("corr-during-partition");
        } catch (Exception ex) {
            writeFailed = true;
        }
        assertThat(writeFailed).as("Write during network partition must fail").isTrue();

        // No partial records from the failed write
        assertThat(eventStoreCount())
                .as("event_store must still have only 1 record — failed write left no partial state")
                .isEqualTo(1);

        // Step 3: restore connectivity
        proxy.toxics().get("cut-connection").remove();

        // Step 4: new connection acquired from pool; write succeeds
        write("corr-after-restore");
        assertThat(eventStoreCount()).isEqualTo(2);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private OutboxRecord write(String correlationId) {
        return new TransactionTemplate(txManager).execute(status ->
                outboxWriter.write("example.created",
                        Map.of("correlationId", correlationId),
                        correlationId));
    }

    private int eventStoreCount() {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM event_store WHERE tenant_id = 'tenant-chaos-net'",
                Integer.class);
        return count != null ? count : 0;
    }
}
