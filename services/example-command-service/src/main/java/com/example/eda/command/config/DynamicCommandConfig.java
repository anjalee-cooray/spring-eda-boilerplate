package com.example.eda.command.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

/**
 * Runtime-refreshable configuration for the command service.
 *
 * Annotated with @RefreshScope so that when /actuator/busrefresh is triggered,
 * Spring Cloud creates a new instance of this bean with the latest values from
 * the config server. Any bean that injects DynamicCommandConfig will get the
 * fresh values on the next method call (via the proxy).
 *
 * How to update config at runtime:
 *   1. Edit config-repo/example-command-service.yml and push to Git
 *   2. POST http://any-service-host/actuator/busrefresh
 *      (Spring Cloud Bus broadcasts the refresh to all instances)
 *   3. This bean is recreated with the new values — no restart needed
 *
 * Note: @Scheduled beans cannot use @RefreshScope because the scheduler holds
 * a direct reference to the bean, bypassing the refresh proxy. For scheduled
 * beans with dynamic config, inject this bean and read values from it at each
 * tick instead of using @Value fields on the scheduled bean itself.
 */
@Component
@RefreshScope
public class DynamicCommandConfig {

    private static final Logger log = LoggerFactory.getLogger(DynamicCommandConfig.class);

    @Value("${app.rate-limit.commands-per-second:100}")
    private int commandsPerSecond;

    @Value("${app.features.example-command-enabled:true}")
    private boolean exampleCommandEnabled;

    public int getCommandsPerSecond() {
        return commandsPerSecond;
    }

    public boolean isExampleCommandEnabled() {
        return exampleCommandEnabled;
    }

    // Called by Spring Cloud after refresh — useful for logging what changed
    @jakarta.annotation.PostConstruct
    public void logConfig() {
        log.info("DynamicCommandConfig loaded: commandsPerSecond={} exampleCommandEnabled={}",
                commandsPerSecond, exampleCommandEnabled);
    }
}
