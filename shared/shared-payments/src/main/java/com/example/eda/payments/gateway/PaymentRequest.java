package com.example.eda.payments.gateway;

import java.util.Map;

/**
 * Request to create a payment intent.
 *
 * idempotencyKey must be generated once per payment attempt by the caller
 * and passed through from the HTTP Idempotency-Key header. Stripe uses it
 * to deduplicate on their side — if the same key is sent twice, Stripe
 * returns the original result without charging the card again.
 *
 * Never reuse an idempotency key for a different payment amount or customer.
 */
public record PaymentRequest(
        long amountInCents,
        String currency,
        String customerId,
        String description,
        Map<String, String> metadata,
        String idempotencyKey
) {
}
