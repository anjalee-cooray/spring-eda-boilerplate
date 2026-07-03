package com.example.eda.payments.gateway;

import java.util.Map;

public record PaymentRequest(
        long amountInCents,
        String currency,
        String customerId,
        String description,
        Map<String, String> metadata
) {
}
