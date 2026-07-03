package com.example.eda.payments.gateway;

/**
 * Pluggable payment gateway interface.
 * Swap implementations via Spring conditional beans — Stripe included, extendable for others.
 */
public interface PaymentGateway {

    PaymentResult createPaymentIntent(PaymentRequest request);

    PaymentResult confirmPayment(String paymentIntentId);

    RefundResult refund(RefundRequest request);

    PaymentResult getPaymentIntent(String paymentIntentId);
}
