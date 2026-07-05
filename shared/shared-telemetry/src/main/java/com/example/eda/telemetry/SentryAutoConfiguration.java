package com.example.eda.telemetry;

import io.sentry.Sentry;
import io.sentry.spring.jakarta.SentrySpringFilter;
import jakarta.servlet.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * Bootstraps Sentry error tracking when app.sentry.dsn is set.
 *
 * Activation requires two conditions:
 *   1. io.sentry.Sentry is on the runtime classpath (service adds the dependency)
 *   2. app.sentry.dsn is set to a non-empty value in the environment
 *
 * Services that want Sentry must declare the dependency in their own build.gradle:
 *   implementation 'io.sentry:sentry-spring-boot-starter-jakarta:7.14.0'
 *
 * Usage:
 *   # application.yml (production only — never commit a real DSN)
 *   app:
 *     sentry:
 *       dsn: ${SENTRY_DSN}
 *       traces-sample-rate: 0.1   # 10% of transactions as performance traces
 *
 * Sensitive data:
 *   PII scrubbing is enabled (sendDefaultPii = false). Request bodies and
 *   stack-local variables are NOT sent unless explicitly attached by the caller.
 */
@AutoConfiguration
@ConditionalOnClass(name = "io.sentry.Sentry")
@ConditionalOnProperty(name = "app.sentry.dsn", matchIfMissing = false)
public class SentryAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SentryAutoConfiguration.class);

    @Bean
    public SentryInitializer sentryInitializer(
            @Value("${app.sentry.dsn}") String dsn,
            @Value("${app.sentry.traces-sample-rate:0.0}") double tracesSampleRate,
            @Value("${app.version:dev}") String release,
            Environment env) {

        String serviceName = env.getProperty("spring.application.name", "unknown");
        String activeProfile = env.getActiveProfiles().length > 0
                ? env.getActiveProfiles()[0]
                : "default";

        return new SentryInitializer(dsn, tracesSampleRate, release, serviceName, activeProfile);
    }

    @Bean
    public Filter sentrySpringFilter() {
        return new SentrySpringFilter();
    }

    /**
     * Thin wrapper that calls Sentry.init() eagerly so Sentry is ready before
     * the first request arrives. Implements AutoCloseable so the Spring context
     * shutdown flushes pending events.
     */
    public static class SentryInitializer implements AutoCloseable {

        private static final Logger log = LoggerFactory.getLogger(SentryInitializer.class);

        SentryInitializer(String dsn, double tracesSampleRate, String release,
                          String serviceName, String environment) {
            Sentry.init(options -> {
                options.setDsn(dsn);
                options.setTracesSampleRate(tracesSampleRate);
                options.setRelease(release);
                options.setEnvironment(environment);
                options.setSendDefaultPii(false); // never send PII to Sentry
                options.setBeforeSend((event, hint) -> {
                    // Tag every event with service name so Sentry issues group by service
                    event.setTag("service", serviceName);
                    return event;
                });
            });

            log.info("Sentry initialised: service={} environment={} release={} tracesSampleRate={}",
                    serviceName, environment, release, tracesSampleRate);
        }

        @Override
        public void close() {
            log.info("Flushing Sentry events before shutdown");
            Sentry.close();
        }
    }
}
