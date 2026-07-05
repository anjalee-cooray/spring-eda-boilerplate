package com.example.eda.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;

/**
 * Spring Cloud Config Server — centralised configuration for all EDA services.
 *
 * Serves configuration from a Git repository (or local filesystem in dev).
 * All client services fetch their config on startup via spring.config.import.
 * Runtime config changes are applied without redeployment:
 *
 *   1. Update the config file in the Git repo and push
 *   2. POST /actuator/busrefresh on any service instance
 *      → Spring Cloud Bus (Kafka-backed) broadcasts a RefreshRemoteApplicationEvent
 *      → All instances reload @RefreshScope beans and @ConfigurationProperties
 *
 * Local dev:
 *   The server defaults to the local config-repo/ directory at the project root.
 *   No Git credentials are needed — it uses a native file backend.
 *   Start with: ./gradlew :services:config-service:bootRun
 *
 * Production:
 *   Set CONFIG_REPO_URI to a private Git repository URI.
 *   Set CONFIG_REPO_USERNAME and CONFIG_REPO_PASSWORD (or use SSH key via GIT_PRIVATE_KEY).
 *   The config-service should be internal-only — not exposed via the public api-gateway.
 */
@SpringBootApplication
@EnableConfigServer
public class ConfigServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConfigServerApplication.class, args);
    }
}
