package com.example.eda.payments.stripe;

public class WebhookVerificationException extends RuntimeException {

    public WebhookVerificationException(String message, Throwable cause) {
        super(message, cause);
    }
}
