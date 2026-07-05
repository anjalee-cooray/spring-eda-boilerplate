package com.example.eda.secrets;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.net.URI;

/**
 * Auto-configuration for the AWS Secrets Manager client bean.
 *
 * The SecretsManagerClient bean is only registered when app.secrets.enabled=true.
 * Services that need to look up secrets at runtime (beyond startup loading) can
 * inject this bean and call getSecretValue() directly.
 *
 * For most use cases the EnvironmentPostProcessor is sufficient — secrets are
 * resolved once at startup and injected as Spring properties. The client bean
 * is useful when secrets need to be rotated at runtime without restarting the
 * service (e.g. rotating DB passwords with zero-downtime connection pool refresh).
 */
@AutoConfiguration
@EnableConfigurationProperties(SecretsManagerProperties.class)
@ConditionalOnProperty(name = "app.secrets.enabled", havingValue = "true")
public class SecretsManagerAutoConfiguration {

    @Bean
    public SecretsManagerClient secretsManagerClient(SecretsManagerProperties props) {
        var builder = SecretsManagerClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.create());

        if (props.endpointOverride() != null && !props.endpointOverride().isBlank()) {
            builder.endpointOverride(URI.create(props.endpointOverride()));
        }

        return builder.build();
    }
}
