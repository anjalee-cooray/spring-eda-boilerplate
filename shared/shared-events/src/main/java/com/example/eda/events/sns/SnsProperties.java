package com.example.eda.events.sns;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.events.sns")
public record SnsProperties(
        String defaultTopicArn,
        Map<String, String> topicArns,
        String region,
        String endpointOverride   // null in production; "http://localhost:4566" for LocalStack
) {

    public String topicArn(String eventType) {
        if (topicArns != null && topicArns.containsKey(eventType)) {
            return topicArns.get(eventType);
        }
        return defaultTopicArn;
    }
}
