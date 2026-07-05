package com.example.eda.secrets;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

class SecretsManagerEnvironmentPostProcessorTest {

    private final SecretsManagerEnvironmentPostProcessor processor =
            new SecretsManagerEnvironmentPostProcessor();

    @Test
    void doesNothingWhenDisabled() {
        MockEnvironment env = new MockEnvironment();
        env.setProperty("app.secrets.enabled", "false");

        // Should not throw — no AWS calls made
        processor.postProcessEnvironment(env, new SpringApplication());

        // No awsSecretsManager source added
        assertThat(env.getPropertySources().contains("awsSecretsManager")).isFalse();
    }

    @Test
    void doesNothingWhenEnabledButNoSecretNamesConfigured() {
        MockEnvironment env = new MockEnvironment();
        env.setProperty("app.secrets.enabled", "true");
        // no app.secrets.secret-names

        // Should not throw (returns after warning log)
        processor.postProcessEnvironment(env, new SpringApplication());

        assertThat(env.getPropertySources().contains("awsSecretsManager")).isFalse();
    }
}
