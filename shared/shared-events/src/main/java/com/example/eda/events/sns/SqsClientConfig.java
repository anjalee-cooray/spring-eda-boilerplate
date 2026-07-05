package com.example.eda.events.sns;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;

/**
 * AWS client beans for SNS (publisher) and SQS (consumer).
 *
 * Credentials are resolved automatically from the AWS default credential chain:
 *   1. Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
 *   2. ECS task role / EC2 instance role (recommended for production)
 *   3. ~/.aws/credentials (local dev without LocalStack)
 *
 * For local development, set app.events.sqs.endpoint-override and
 * app.events.sns.endpoint-override to the LocalStack URL (http://localhost:4566).
 * LocalStack emulates SNS and SQS locally without an AWS account.
 *
 * To switch a service from Kafka to SNS/SQS:
 *   app.events.broker: sns   (was: kafka)
 */
@Configuration
@ConditionalOnProperty(name = "app.events.broker", havingValue = "sns")
public class SqsClientConfig {

    @Bean
    public SqsClient sqsClient(SqsProperties sqsProperties) {
        SqsClient.Builder builder = SqsClient.builder()
                .region(Region.of(sqsProperties.region()));

        if (sqsProperties.endpointOverride() != null && !sqsProperties.endpointOverride().isBlank()) {
            builder.endpointOverride(URI.create(sqsProperties.endpointOverride()));
        }

        return builder.build();
    }

    @Bean
    public SnsClient snsClient(SnsProperties snsProperties) {
        SnsClient.Builder builder = SnsClient.builder()
                .region(Region.of(snsProperties.region()));

        if (snsProperties.endpointOverride() != null && !snsProperties.endpointOverride().isBlank()) {
            builder.endpointOverride(URI.create(snsProperties.endpointOverride()));
        }

        return builder.build();
    }
}
