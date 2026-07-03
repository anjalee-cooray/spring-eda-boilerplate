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

    public StripePaymentGateway(StripeProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void init() {
        Stripe.apiKey = properties.secretKey();
    }

    @Override
    public PaymentResult createPaymentIntent(PaymentRequest request) {
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

    @Override
    public PaymentResult confirmPayment(String paymentIntentId) {
        try {
            PaymentIntent intent = PaymentIntent.retrieve(paymentIntentId);
            intent = intent.confirm(PaymentIntentConfirmParams.builder().build());
            log.debug("Confirmed PaymentIntent id={} status={}", intent.getId(), intent.getStatus());
            return toResult(intent);
        } catch (StripeException e) {
            throw new PaymentException("Failed to confirm PaymentIntent", paymentIntentId, e);
        }
    }

    @Override
    public RefundResult refund(RefundRequest request) {
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

    @Override
    public PaymentResult getPaymentIntent(String paymentIntentId) {
        try {
            PaymentIntent intent = PaymentIntent.retrieve(
                    paymentIntentId, PaymentIntentRetrieveParams.builder().build(), null);
            return toResult(intent);
        } catch (StripeException e) {
            throw new PaymentException("Failed to retrieve PaymentIntent", paymentIntentId, e);
        }
    }

    private PaymentResult toResult(PaymentIntent intent) {
        return new PaymentResult(
                intent.getId(),
                intent.getClientSecret(),
                mapStatus(intent.getStatus())
        );
    }

    private RefundResult toRefundResult(Refund refund, String paymentIntentId) {
        return new RefundResult(
                refund.getId(),
                paymentIntentId,
                refund.getAmount(),
                mapRefundStatus(refund.getStatus())
        );
    }

    private PaymentResult.PaymentStatus mapStatus(String status) {
        return switch (status) {
            case "requires_payment_method" -> PaymentResult.PaymentStatus.REQUIRES_PAYMENT_METHOD;
            case "requires_confirmation" -> PaymentResult.PaymentStatus.REQUIRES_CONFIRMATION;
            case "requires_action" -> PaymentResult.PaymentStatus.REQUIRES_ACTION;
            case "processing" -> PaymentResult.PaymentStatus.PROCESSING;
            case "succeeded" -> PaymentResult.PaymentStatus.SUCCEEDED;
            case "canceled" -> PaymentResult.PaymentStatus.CANCELLED;
            default -> PaymentResult.PaymentStatus.FAILED;
        };
    }

    private RefundResult.RefundStatus mapRefundStatus(String status) {
        return switch (status) {
            case "pending" -> RefundResult.RefundStatus.PENDING;
            case "succeeded" -> RefundResult.RefundStatus.SUCCEEDED;
            case "failed" -> RefundResult.RefundStatus.FAILED;
            case "canceled" -> RefundResult.RefundStatus.CANCELLED;
            default -> RefundResult.RefundStatus.FAILED;
        };
    }
}
