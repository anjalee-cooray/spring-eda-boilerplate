package com.example.eda.payments.gateway;

/**
 * Request to refund a payment intent.
 *
 * idempotencyKey must be generated once per refund attempt by the caller.
 * Stripe deduplicates refunds on their side using this key — the same refund
 * will not be issued twice even if the request is retried after a network failure.
 */
public record RefundRequest(
        String paymentIntentId,
        long amountInCents,
        String reason,
        String idempotencyKey
) {
}
