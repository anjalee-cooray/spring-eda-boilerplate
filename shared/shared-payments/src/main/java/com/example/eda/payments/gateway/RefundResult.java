package com.example.eda.payments.gateway;

public record RefundResult(
        String refundId,
        String paymentIntentId,
        long amountInCents,
        RefundStatus status
) {

    public enum RefundStatus {
        PENDING,
        SUCCEEDED,
        FAILED,
        CANCELLED
    }
}
