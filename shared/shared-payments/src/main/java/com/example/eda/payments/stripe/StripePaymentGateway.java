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
import com.stripe.net.RequestOptions;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import jakarta.annotation.PostConstruct;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Stripe implementation of PaymentGateway.
 *
 * Resilience decorator order (outermost → innermost):
 *   CircuitBreaker → Retry → Stripe API call
 *
 * The circuit breaker sees the final outcome after all retry attempts are
 * exhausted — not each individual attempt. This means transient failures
 * that succeed on retry do not count as failures toward the circuit breaker
 * threshold.
 *
 * Only network-level exceptions trigger retries (IOException, ConnectException,
 * TimeoutException). Business errors (PaymentException wrapping StripeException)
 * are not retried — a declined card will not succeed on a second attempt.
 */
@Component
@ConditionalOnProperty(name = "app.payments.provider", havingValue = "stripe", matchIfMissing = true)
public class StripePaymentGateway implements PaymentGateway {

    private static final Logger log = LoggerFactory.getLogger(StripePaymentGateway.class);

    private final StripeProperties properties;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;

    public StripePaymentGateway(
            StripeProperties properties,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry) {
        this.properties = properties;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("stripe");
        this.retry = retryRegistry.retry("stripe");
    }

    @PostConstruct
    public void init() {
        Stripe.apiKey = properties.secretKey();
    }

    @Override
    public PaymentResult createPaymentIntent(PaymentRequest request) {
        return execute("createPaymentIntent", () -> doCreatePaymentIntent(request));
    }

    @Override
    public PaymentResult confirmPayment(String paymentIntentId) {
        return execute("confirmPayment", () -> doConfirmPayment(paymentIntentId));
    }

    @Override
    public RefundResult refund(RefundRequest request) {
        return execute("refund", () -> doRefund(request));
    }

    @Override
    public PaymentResult getPaymentIntent(String paymentIntentId) {
        return execute("getPaymentIntent", () -> doGetPaymentIntent(paymentIntentId));
    }

    /**
     * Applies CircuitBreaker → Retry decoration in the correct order.
     * The circuit breaker is the outermost decorator so it evaluates
     * the final result after all retry attempts are exhausted.
     */
    private <T> T execute(String operation, Supplier<T> call) {
        Supplier<T> withRetry = Retry.decorateSupplier(retry, call);
        Supplier<T> withCircuitBreaker = CircuitBreaker.decorateSupplier(circuitBreaker, withRetry);
        try {
            return withCircuitBreaker.get();
        } catch (CallNotPermittedException e) {
            log.warn("Circuit breaker OPEN — Stripe {} rejected", operation);
            throw new PaymentException("Payment service temporarily unavailable — please retry later", e);
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
            // Idempotency key passed to Stripe — if this request is retried after a
            // network failure, Stripe returns the original result without charging again.
            RequestOptions options = requestOptions(request.idempotencyKey());
            PaymentIntent intent = PaymentIntent.create(params, options);
            log.debug("Created PaymentIntent id={} status={}", intent.getId(), intent.getStatus());
            return toResult(intent);
        } catch (StripeException e) {
            throw new PaymentException("Failed to create PaymentIntent", e);
        }
    }

    private PaymentResult doConfirmPayment(String paymentIntentId) {
        try {
            PaymentIntent intent = PaymentIntent.retrieve(paymentIntentId);
            // confirmPayment is keyed on paymentIntentId itself — Stripe deduplicates
            // confirm calls for the same intent automatically.
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
            // Idempotency key prevents double-refund if the response is lost in transit.
            RequestOptions options = requestOptions(request.idempotencyKey());
            Refund refund = Refund.create(params, options);
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

    private RequestOptions requestOptions(String idempotencyKey) {
        RequestOptions.RequestOptionsBuilder builder = RequestOptions.builder();
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            builder.setIdempotencyKey(idempotencyKey);
        }
        return builder.build();
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
