package com.example.eda.payments.stripe;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.payments.stripe")
public record StripeProperties(
        String secretKey,
        String webhookSecret,
        String defaultCurrency
) {
}
