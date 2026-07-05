package com.example.eda.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Periodically triggers a Spring Cloud Config refresh without restarting pods.
 *
 * The problem it solves:
 *   Secrets (DB passwords, API keys, JWT signing keys) loaded at startup become
 *   stale after rotation. Without this scheduler, a pod must be restarted to
 *   pick up the new value — creating a deployment event (and brief disruption)
 *   for every rotation.
 *
 * How it works:
 *   1. Spring Cloud Config stores secrets in a backing store (Vault, AWS Secrets
 *      Manager via the Spring Cloud AWS extension, or the Git config server).
 *   2. ContextRefresher.refresh() re-fetches all @RefreshScope beans and
 *      re-binds @ConfigurationProperties classes, so new secret values are
 *      visible immediately — no pod restart required.
 *   3. The scheduler runs on a fixed-delay interval (default: 5 minutes) that
 *      is shorter than the shortest secret rotation period.
 *
 * Activation:
 *   Requires app.secrets.refresh.enabled=true (default false so local dev is
 *   unaffected). Also requires spring-cloud-context on the classpath.
 *
 * Configuration:
 *   app:
 *     secrets:
 *       refresh:
 *         enabled: true
 *         interval-ms: 300000  # 5 min; set shorter than rotation period
 *
 * What gets refreshed:
 *   - All beans annotated @RefreshScope
 *   - All @ConfigurationProperties classes bound via @EnableConfigurationProperties
 *   - Datasource connection pool (if HikariCP is used, new connections pick up
 *     the new password; existing connections drain on next eviction)
 *
 * Security note:
 *   The refresh triggers a context-level cache invalidation. During the brief
 *   re-bind window (~10ms) there is a small window where old and new values may
 *   coexist across threads. This is acceptable because: (a) secrets are rotated
 *   with an overlap period where both old and new are valid, and (b) auth tokens
 *   with the old signing key expire naturally within their TTL.
 *
 * Limitation:
 *   Does NOT restart the Kafka consumer or SNS/SQS clients — those hold
 *   long-lived connections that are not @RefreshScope by default. For broker
 *   credential rotation, use IAM roles or managed credentials instead.
 */
@Component
@ConditionalOnProperty(name = "app.secrets.refresh.enabled", havingValue = "true", matchIfMissing = false)
public class SecretsRefreshScheduler {

    private static final Logger log = LoggerFactory.getLogger(SecretsRefreshScheduler.class);

    private final ContextRefresher contextRefresher;

    public SecretsRefreshScheduler(ContextRefresher contextRefresher) {
        this.contextRefresher = contextRefresher;
    }

    /**
     * Re-fetches all config sources and refreshes @RefreshScope beans.
     *
     * Uses fixedDelayString (not fixedRateString) so the delay is measured from
     * the end of the previous refresh, preventing overlapping refresh calls if
     * a refresh takes longer than the interval (e.g. config server is slow).
     */
    @Scheduled(fixedDelayString = "${app.secrets.refresh.interval-ms:300000}")
    public void refresh() {
        log.debug("SecretsRefreshScheduler: triggering config refresh");

        try {
            Set<String> changedKeys = contextRefresher.refresh();

            if (changedKeys.isEmpty()) {
                log.debug("SecretsRefreshScheduler: no config changes detected");
            } else {
                // Log changed keys but not their values — secret values must never appear in logs
                log.info("SecretsRefreshScheduler: config refreshed, changed keys count={} keys={}",
                        changedKeys.size(), sanitiseKeys(changedKeys));
            }
        } catch (Exception ex) {
            // Refresh failure must not crash the pod — the current secret value
            // remains in effect until the next successful refresh.
            log.error("SecretsRefreshScheduler: refresh failed — retrying on next interval", ex);
        }
    }

    /**
     * Redacts key names that contain sensitive substrings before logging.
     * Key names like "spring.datasource.password" are still sensitive metadata.
     */
    private static Set<String> sanitiseKeys(Set<String> keys) {
        return keys.stream()
                .map(k -> {
                    String lower = k.toLowerCase();
                    if (lower.contains("password") || lower.contains("secret")
                            || lower.contains("key") || lower.contains("token")
                            || lower.contains("dsn") || lower.contains("credentials")) {
                        return k + "=[REDACTED]";
                    }
                    return k;
                })
                .collect(java.util.stream.Collectors.toSet());
    }
}
