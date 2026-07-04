package com.example.eda.payments.stripe;

import com.example.eda.payments.gateway.PaymentException;
import com.example.eda.payments.gateway.PaymentGateway;
import com.example.eda.payments.gateway.PaymentRequest;
import com.example.eda.payments.gateway.PaymentResult;
import com.example.eda.payments.gateway.RefundRequest;
import com.example.eda.payments.gateway.RefundResult;
import com.stripe.Stripe;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import com.stripe.param.PaymentIntentConfirmParams;
import com.stripe.param.PaymentIntentCreateParams;
import com.stripe.param.PaymentIntentRetrieveParams;
import com.stripe.param.RefundCreateParams;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "app.payments.provider", havingValue = "stripe", matchIfMissing = true)
public class StripePaymentGateway implements PaymentGateway {

    private static final Logger log = LoggerFactory.getLogger(StripePaymentGateway.class);

    private final StripeProperties properties;
    private final CircuitBreaker circuitBreaker;

    public StripePaymentGateway(StripeProperties properties, CircuitBreakerRegistry registry) {
        this.properties = properties;
        this.circuitBreaker = registry.circuitBreaker("stripe");
    }

    @PostConstruct
    public void init() {
        Stripe.apiKey = properties.secretKey();
    }

    @Override
    public PaymentResult createPaymentIntent(PaymentRequest request) {
        try {
            return circuitBreaker.executeSupplier(() -> doCreatePaymentIntent(request));
        } catch (CallNotPermittedException e) {
            log.warn("Circuit breaker OPEN — Stripe createPaymentIntent rejected");
            throw new PaymentException("Payment service temporarily unavailable", e);
        }
    }

    @Override
    public PaymentResult confirmPayment(String paymentIntentId) {
        try {
            return circuitBreaker.executeSupplier(() -> doConfirmPayment(paymentIntentId));
        } catch (CallNotPermittedException e) {
            log.warn("Circuit breaker OPEN — Stripe confirmPayment rejected id={}", paymentIntentId);
            throw new PaymentException("Payment service temporarily unavailable", e);
        }
    }

    @Override
    public RefundResult refund(RefundRequest request) {
        try {
            return circuitBreaker.executeSupplier(() -> doRefund(request));
        } catch (CallNotPermittedException e) {
            log.warn("Circuit breaker OPEN — Stripe refund rejected paymentIntentId={}", request.paymentIntentId());
            throw new PaymentException("Payment service temporarily unavailable", e);
        }
    }

    @Override
    public PaymentResult getPaymentIntent(String paymentIntentId) {
        try {
            return circuitBreaker.executeSupplier(() -> doGetPaymentIntent(paymentIntentId));
        } catch (CallNotPermittedException e) {
            log.warn("Circuit breaker OPEN — Stripe getPaymentIntent rejected id={}", paymentIntentId);
            throw new PaymentException("Payment service temporarily unavailable", e);
        }
    }

    private PaymentResult doCreatePaymentIntent(PaymentRequest request) {
        try {
            PaymentIntentCreateParams params = PaymentIntentCreateParams.builder()
                    .setAmount(request.amountInCents())
                    .setCurrency(request.currency())
                    .setCustomer(request.customerId())
                    .setDescription(request.description())
                    .putAllMetadata(request.metadata())
                    .build();
            PaymentIntent intent = PaymentIntent.create(params);
            log.debug("Created PaymentIntent id={} status={}", intent.getId(), intent.getStatus());
            return toResult(intent);
        } catch (StripeException e) {
            throw new PaymentException("Failed to create PaymentIntent", e);
        }
    }

    private PaymentResult doConfirmPayment(String paymentIntentId) {
        try {
            PaymentIntent intent = PaymentIntent.retrieve(paymentIntentId);
            intent = intent.confirm(PaymentIntentConfirmParams.builder().build());
            log.debug("Confirmed PaymentIntent id={} status={}", intent.getId(), intent.getStatus());
            return toResult(intent);
        } catch (StripeException e) {
            throw new PaymentException("Failed to confirm PaymentIntent", paymentIntentId, e);
        }
    }

    private RefundResult doRefund(RefundRequest request) {
        try {
            RefundCreateParams params = RefundCreateParams.builder()
                    .setPaymentIntent(request.paymentIntentId())
                    .setAmount(request.amountInCents())
                    .setReason(RefundCreateParams.Reason.valueOf(request.reason()))
                    .build();
            Refund refund = Refund.create(params);
            log.debug("Created Refund id={} status={}", refund.getId(), refund.getStatus());
            return toRefundResult(refund, request.paymentIntentId());
        } catch (StripeException e) {
            throw new PaymentException("Failed to create Refund", request.paymentIntentId(), e);
        }
    }

    private PaymentResult doGetPaymentIntent(String paymentIntentId) {
        try {
            PaymentIntent intent = PaymentIntent.retrieve(
                    paymentIntentId, PaymentIntentRetrieveParams.builder().build(), null);
            return toResult(intent);
        } catch (StripeException e) {
            throw new PaymentException("Failed to retrieve PaymentIntent", paymentIntentId, e);
        }
    }

    private PaymentResult toResult(PaymentIntent intent) {
        return new PaymentResult(intent.getId(), intent.getClientSecret(), mapStatus(intent.getStatus()));
    }

    private RefundResult toRefundResult(Refund refund, String paymentIntentId) {
        return new RefundResult(refund.getId(), paymentIntentId, refund.getAmount(), mapRefundStatus(refund.getStatus()));
    }

    private PaymentResult.PaymentStatus mapStatus(String status) {
        return switch (status) {
            case "requires_payment_method" -> PaymentResult.PaymentStatus.REQUIRES_PAYMENT_METHOD;
            case "requires_confirmation"   -> PaymentResult.PaymentStatus.REQUIRES_CONFIRMATION;
            case "requires_action"         -> PaymentResult.PaymentStatus.REQUIRES_ACTION;
            case "processing"              -> PaymentResult.PaymentStatus.PROCESSING;
            case "succeeded"               -> PaymentResult.PaymentStatus.SUCCEEDED;
            case "canceled"                -> PaymentResult.PaymentStatus.CANCELLED;
            default                        -> PaymentResult.PaymentStatus.FAILED;
        };
    }

    private RefundResult.RefundStatus mapRefundStatus(String status) {
        return switch (status) {
            case "pending"   -> RefundResult.RefundStatus.PENDING;
            case "succeeded" -> RefundResult.RefundStatus.SUCCEEDED;
            case "failed"    -> RefundResult.RefundStatus.FAILED;
            case "canceled"  -> RefundResult.RefundStatus.CANCELLED;
            default          -> RefundResult.RefundStatus.FAILED;
        };
    }
}
