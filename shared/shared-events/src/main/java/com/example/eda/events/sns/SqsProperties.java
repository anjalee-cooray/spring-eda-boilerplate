package com.example.eda.events.sns;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.events.sqs")
public record SqsProperties(
        String queueUrl,
        int pollIntervalMs
) {
}
