package com.example.eda.payments.gateway;

public record PaymentResult(
        String paymentIntentId,
        String clientSecret,
        PaymentStatus status
) {

    public enum PaymentStatus {
        REQUIRES_PAYMENT_METHOD,
        REQUIRES_CONFIRMATION,
        REQUIRES_ACTION,
        PROCESSING,
        SUCCEEDED,
        CANCELLED,
        FAILED
    }
}
