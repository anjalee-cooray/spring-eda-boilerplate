package com.example.eda.query.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

/**
 * Runtime-refreshable configuration for the query service.
 * See DynamicCommandConfig for the refresh pattern explanation.
 */
@Component
@RefreshScope
public class DynamicQueryConfig {

    private static final Logger log = LoggerFactory.getLogger(DynamicQueryConfig.class);

    @Value("${app.query.max-page-size:100}")
    private int maxPageSize;

    @Value("${app.query.default-page-size:20}")
    private int defaultPageSize;

    @Value("${app.features.example-query-enabled:true}")
    private boolean exampleQueryEnabled;

    public int getMaxPageSize() {
        return maxPageSize;
    }

    public int getDefaultPageSize() {
        return defaultPageSize;
    }

    public boolean isExampleQueryEnabled() {
        return exampleQueryEnabled;
    }

    @jakarta.annotation.PostConstruct
    public void logConfig() {
        log.info("DynamicQueryConfig loaded: maxPageSize={} defaultPageSize={} queryEnabled={}",
                maxPageSize, defaultPageSize, exampleQueryEnabled);
    }
}
