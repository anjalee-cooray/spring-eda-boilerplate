package com.example.eda.events.sns;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.events.sqs")
public record SqsProperties(
        String queueUrl,
        String dlqQueueUrl,
        int pollIntervalMs,
        String region,
        String endpointOverride   // null in production; "http://localhost:4566" for LocalStack
) {
}
