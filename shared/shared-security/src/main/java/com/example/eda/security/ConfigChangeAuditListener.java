package com.example.eda.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.context.environment.EnvironmentChangeEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Produces a structured audit log entry whenever Spring Cloud Config detects
 * a configuration change (via refresh or Spring Cloud Bus broadcast).
 *
 * Why this exists:
 *   Config drift is a common source of production incidents. Without an audit
 *   trail, answering "what changed and when?" after an incident requires
 *   piecing together git history, deploy logs, and manual memory. This listener
 *   writes a machine-readable audit entry to the structured log stream (picked
 *   up by Loki/Grafana) every time the live config changes.
 *
 * What it captures:
 *   - Timestamp of the change
 *   - Set of changed key names (redacted for sensitive keys)
 *   - Service name (from MDC, set by MdcFilter at request time)
 *
 * What it does NOT capture:
 *   - Old or new values — logging secret values would be a security violation.
 *     If value-level audit is required, use AWS Config or Vault audit logs,
 *     which are protected stores designed for secret auditing.
 *
 * Activation:
 *   This bean is active whenever spring-cloud-context is on the classpath
 *   (it is in all services that depend on shared-security and use Spring Cloud).
 *   No extra configuration is needed.
 *
 * Downstream:
 *   The log line uses JSON format (via logstash-logback-encoder) so Loki can
 *   index on "event=config_changed" and "service" for alerts and dashboards.
 *
 * Correlation with SecretsRefreshScheduler:
 *   This listener fires after every successful SecretsRefreshScheduler.refresh()
 *   call that detects changed keys — giving a chronological audit of all
 *   secret rotations without any manual instrumentation.
 */
@Component
@ConditionalOnClass(name = "org.springframework.cloud.context.environment.EnvironmentChangeEvent")
public class ConfigChangeAuditListener {

    private static final Logger log = LoggerFactory.getLogger(ConfigChangeAuditListener.class);

    private static final Set<String> SENSITIVE_SUBSTRINGS = Set.of(
            "password", "secret", "key", "token", "dsn", "credentials", "cert", "private"
    );

    @EventListener
    public void onEnvironmentChanged(EnvironmentChangeEvent event) {
        Set<String> keys = event.getKeys();
        if (keys == null || keys.isEmpty()) {
            return;
        }

        Set<String> sanitisedKeys = keys.stream()
                .map(ConfigChangeAuditListener::sanitiseKey)
                .collect(Collectors.toSet());

        // Structured log — logstash-logback-encoder serialises MDC fields as
        // top-level JSON properties, making this queryable in Loki/Grafana.
        log.info("event=config_changed changedKeyCount={} changedKeys={} occurredAt={}",
                keys.size(), sanitisedKeys, Instant.now());
    }

    /**
     * Returns the key name with "[REDACTED]" appended if it contains a sensitive
     * substring. The key name itself is retained so operators know which property
     * category changed; only the value is never recorded.
     */
    private static String sanitiseKey(String key) {
        String lower = key.toLowerCase();
        for (String sensitive : SENSITIVE_SUBSTRINGS) {
            if (lower.contains(sensitive)) {
                return key + "=[REDACTED]";
            }
        }
        return key;
    }
}
