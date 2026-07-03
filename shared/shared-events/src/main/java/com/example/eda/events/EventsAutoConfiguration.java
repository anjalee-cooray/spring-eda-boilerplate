package com.example.eda.events;

import com.example.eda.events.sns.SnsProperties;
import com.example.eda.events.sns.SqsProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration
@ComponentScan(basePackages = "com.example.eda.events")
@EnableScheduling
@EnableConfigurationProperties({SnsProperties.class, SqsProperties.class})
public class EventsAutoConfiguration {
}
