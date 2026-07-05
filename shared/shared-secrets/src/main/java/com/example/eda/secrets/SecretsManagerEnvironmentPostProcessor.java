package com.example.eda.secrets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

/**
 * Loads secrets from AWS Secrets Manager before the Spring application context starts.
 *
 * Runs as an EnvironmentPostProcessor — the earliest hook available in the Spring
 * lifecycle — so that all subsequent beans see the resolved secret values as ordinary
 * Spring properties. No changes are needed in application.yml: existing
 * ${spring.datasource.password} references resolve automatically.
 *
 * Activation:
 *   Set app.secrets.enabled=true and list secret names in app.secrets.secret-names.
 *   Disabled by default so local dev needs no AWS credentials.
 *
 * Secret format (Secrets Manager):
 *   Store each secret as a flat JSON object. Keys become Spring property names:
 *   {
 *     "spring.datasource.password": "s3cr3t",
 *     "stripe.api.key":             "sk_live_abc123"
 *   }
 *
 * Priority:
 *   Secrets are added as a PropertySource named "awsSecretsManager" with higher
 *   priority than application.yml but lower than system environment variables.
 *   This means SPRING_DATASOURCE_PASSWORD env var still overrides Secrets Manager,
 *   which is useful for emergency overrides without redeployment.
 *
 * IAM:
 *   The service needs secretsmanager:GetSecretValue on each listed secret ARN.
 *   Use ECS task roles or EC2 instance profiles — never embed credentials.
 *
 * Local dev with LocalStack:
 *   Set app.secrets.endpoint-override=http://localhost:4566
 *   aws --endpoint-url=http://localhost:4566 secretsmanager create-secret \
 *     --name /eda/dev/db --secret-string '{"spring.datasource.password":"postgres"}'
 */
public class SecretsManagerEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    private static final Logger log = LoggerFactory.getLogger(SecretsManagerEnvironmentPostProcessor.class);
    private static final String PROPERTY_SOURCE_NAME = "awsSecretsManager";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        boolean enabled = environment.getProperty("app.secrets.enabled", Boolean.class, false);
        if (!enabled) {
            log.debug("AWS Secrets Manager integration disabled (app.secrets.enabled=false)");
            return;
        }

        String region = environment.getProperty("app.secrets.region", "us-east-1");
        String endpointOverride = environment.getProperty("app.secrets.endpoint-override", "");
        List<String> secretNames = resolveSecretNames(environment);

        if (secretNames.isEmpty()) {
            log.warn("AWS Secrets Manager enabled but no secret names configured (app.secrets.secret-names)");
            return;
        }

        log.info("Loading {} secret(s) from AWS Secrets Manager region={}", secretNames.size(), region);

        try (SecretsManagerClient client = buildClient(region, endpointOverride)) {
            Map<String, Object> properties = new LinkedHashMap<>();

            for (String secretName : secretNames) {
                loadSecret(client, secretName, properties);
            }

            if (!properties.isEmpty()) {
                // Insert AFTER system env vars (index 1) so env vars can still override secrets.
                // System env vars sit at index 0 in the property sources list.
                MapPropertySource secretsSource = new MapPropertySource(PROPERTY_SOURCE_NAME, properties);
                environment.getPropertySources().addAfter("systemEnvironment", secretsSource);
                log.info("Injected {} properties from AWS Secrets Manager", properties.size());
            }
        } catch (Exception ex) {
            // Fail fast — a missing secret in production is a startup error, not a warning.
            throw new IllegalStateException(
                    "Failed to load secrets from AWS Secrets Manager. "
                    + "Check IAM permissions and network connectivity. "
                    + "Set app.secrets.enabled=false to disable for local dev.", ex);
        }
    }

    private void loadSecret(SecretsManagerClient client, String secretName,
                            Map<String, Object> target) {
        try {
            GetSecretValueResponse response = client.getSecretValue(
                    GetSecretValueRequest.builder().secretId(secretName).build());

            String secretString = response.secretString();
            if (secretString == null || secretString.isBlank()) {
                log.warn("Secret {} returned an empty value — skipping", secretName);
                return;
            }

            if (secretString.trim().startsWith("{")) {
                // JSON object → flatten keys into Spring properties
                Map<String, Object> parsed = MAPPER.readValue(secretString,
                        new TypeReference<Map<String, Object>>() { });
                target.putAll(parsed);
                log.debug("Loaded {} properties from secret {}", parsed.size(), secretName);
            } else {
                // Plain string secret — expose under the secret name itself
                target.put(secretName, secretString);
                log.debug("Loaded plain-string secret {}", secretName);
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to load secret: " + secretName, ex);
        }
    }

    private SecretsManagerClient buildClient(String region, String endpointOverride) {
        SecretsManagerClientBuilder builder = SecretsManagerClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create());

        if (endpointOverride != null && !endpointOverride.isBlank()) {
            builder.endpointOverride(URI.create(endpointOverride));
            log.info("AWS Secrets Manager endpoint overridden to {}", endpointOverride);
        }

        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private List<String> resolveSecretNames(ConfigurableEnvironment environment) {
        String raw = environment.getProperty("app.secrets.secret-names", "");
        if (raw.isBlank()) return List.of();
        return List.of(raw.split(",")).stream()
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .toList();
    }

    @Override
    public int getOrder() {
        // Run after ConfigFileApplicationListener (loads application.yml) so we can
        // read app.secrets.* config, but before most other post-processors.
        return Ordered.LOWEST_PRECEDENCE - 100;
    }
}
