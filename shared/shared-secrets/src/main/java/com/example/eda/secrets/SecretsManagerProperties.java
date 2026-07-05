package com.example.eda.secrets;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for AWS Secrets Manager integration.
 *
 * app.secrets.enabled          — false by default; set true in production
 * app.secrets.secret-names     — list of secret ARNs or names to load at startup
 * app.secrets.region           — AWS region (defaults to us-east-1)
 * app.secrets.endpoint-override — LocalStack endpoint for local dev (leave blank in prod)
 * app.secrets.cache-ttl-seconds — how long resolved values are cached before re-fetch
 *
 * Each secret must be stored in Secrets Manager as a flat JSON object:
 *   {"spring.datasource.password": "s3cr3t", "stripe.api.key": "sk_live_..."}
 *
 * The keys are injected as Spring properties with the same names, so existing
 * ${spring.datasource.password} references in application.yml work without change.
 */
@ConfigurationProperties(prefix = "app.secrets")
public record SecretsManagerProperties(
        boolean enabled,
        List<String> secretNames,
        String region,
        String endpointOverride,
        long cacheTtlSeconds
) {
    public SecretsManagerProperties {
        if (secretNames == null) secretNames = List.of();
        if (region == null || region.isBlank()) region = "us-east-1";
        if (cacheTtlSeconds <= 0) cacheTtlSeconds = 300;
    }
}
