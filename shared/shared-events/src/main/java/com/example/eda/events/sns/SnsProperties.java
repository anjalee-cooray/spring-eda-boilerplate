package com.example.eda.events.sns;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.events.sns")
public record SnsProperties(
        String defaultTopicArn,
        Map<String, String> topicArns
) {

    public String topicArn(String eventType) {
        if (topicArns != null && topicArns.containsKey(eventType)) {
            return topicArns.get(eventType);
        }
        return defaultTopicArn;
    }
}
