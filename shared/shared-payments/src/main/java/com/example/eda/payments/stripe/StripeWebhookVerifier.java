package com.example.eda.payments.stripe;

import com.stripe.exception.SignatureVerificationException;
import com.stripe.model.Event;
import com.stripe.net.Webhook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class StripeWebhookVerifier {

    private static final Logger log = LoggerFactory.getLogger(StripeWebhookVerifier.class);

    private final StripeProperties properties;

    public StripeWebhookVerifier(StripeProperties properties) {
        this.properties = properties;
    }

    /**
     * Verifies the Stripe-Signature header and returns the parsed Event.
     * Throws WebhookVerificationException if the signature is invalid.
     */
    public Event verify(String payload, String sigHeader) {
        try {
            return Webhook.constructEvent(payload, sigHeader, properties.webhookSecret());
        } catch (SignatureVerificationException e) {
            log.warn("Invalid Stripe webhook signature");
            throw new WebhookVerificationException("Invalid Stripe webhook signature", e);
        }
    }
}
