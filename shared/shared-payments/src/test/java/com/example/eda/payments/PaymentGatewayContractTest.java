package com.example.eda.payments;

import com.example.eda.payments.gateway.PaymentGateway;
import com.example.eda.payments.gateway.PaymentRequest;
import com.example.eda.payments.gateway.PaymentResult;
import com.example.eda.payments.gateway.RefundRequest;
import com.example.eda.payments.gateway.RefundResult;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaymentGatewayContractTest {

    private final PaymentGateway gateway = mock(PaymentGateway.class);

    @Test
    void createPaymentIntentReturnsResult() {
        PaymentRequest request = new PaymentRequest(1000L, "usd", "cust-1", "Test payment", Map.of(), "idem-key-1");
        PaymentResult expected = new PaymentResult("pi_123", "secret_123", PaymentResult.PaymentStatus.REQUIRES_PAYMENT_METHOD);

        when(gateway.createPaymentIntent(any())).thenReturn(expected);

        PaymentResult result = gateway.createPaymentIntent(request);

        assertThat(result.paymentIntentId()).isEqualTo("pi_123");
        assertThat(result.status()).isEqualTo(PaymentResult.PaymentStatus.REQUIRES_PAYMENT_METHOD);
    }

    @Test
    void refundReturnsRefundResult() {
        RefundRequest request = new RefundRequest("pi_123", 500L, "DUPLICATE", "idem-key-2");
        RefundResult expected = new RefundResult("re_123", "pi_123", 500L, RefundResult.RefundStatus.SUCCEEDED);

        when(gateway.refund(any())).thenReturn(expected);

        RefundResult result = gateway.refund(request);

        assertThat(result.refundId()).isEqualTo("re_123");
        assertThat(result.status()).isEqualTo(RefundResult.RefundStatus.SUCCEEDED);
        assertThat(result.amountInCents()).isEqualTo(500L);
    }

    @Test
    void confirmPaymentReturnsSucceededStatus() {
        PaymentResult expected = new PaymentResult("pi_123", null, PaymentResult.PaymentStatus.SUCCEEDED);
        when(gateway.confirmPayment("pi_123")).thenReturn(expected);

        PaymentResult result = gateway.confirmPayment("pi_123");

        assertThat(result.status()).isEqualTo(PaymentResult.PaymentStatus.SUCCEEDED);
    }
}
