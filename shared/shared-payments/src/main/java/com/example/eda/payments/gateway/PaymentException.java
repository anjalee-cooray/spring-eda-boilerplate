package com.example.eda.payments.gateway;

public class PaymentException extends RuntimeException {

    private final String paymentIntentId;

    public PaymentException(String message, Throwable cause) {
        super(message, cause);
        this.paymentIntentId = null;
    }

    public PaymentException(String message, String paymentIntentId, Throwable cause) {
        super(message, cause);
        this.paymentIntentId = paymentIntentId;
    }

    public String getPaymentIntentId() {
        return paymentIntentId;
    }
}
