package com.example.eda.payments.gateway;

public record RefundRequest(
        String paymentIntentId,
        long amountInCents,
        String reason
) {
}
