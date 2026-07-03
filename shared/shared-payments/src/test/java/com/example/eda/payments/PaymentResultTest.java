package com.example.eda.payments;

import com.example.eda.payments.gateway.PaymentResult;
import com.example.eda.payments.gateway.RefundResult;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentResultTest {

    @Test
    void paymentResultStatusEnumCoversAllStates() {
        assertThat(PaymentResult.PaymentStatus.values()).containsExactlyInAnyOrder(
                PaymentResult.PaymentStatus.REQUIRES_PAYMENT_METHOD,
                PaymentResult.PaymentStatus.REQUIRES_CONFIRMATION,
                PaymentResult.PaymentStatus.REQUIRES_ACTION,
                PaymentResult.PaymentStatus.PROCESSING,
                PaymentResult.PaymentStatus.SUCCEEDED,
                PaymentResult.PaymentStatus.CANCELLED,
                PaymentResult.PaymentStatus.FAILED
        );
    }

    @Test
    void refundResultStatusEnumCoversAllStates() {
        assertThat(RefundResult.RefundStatus.values()).containsExactlyInAnyOrder(
                RefundResult.RefundStatus.PENDING,
                RefundResult.RefundStatus.SUCCEEDED,
                RefundResult.RefundStatus.FAILED,
                RefundResult.RefundStatus.CANCELLED
        );
    }

    @Test
    void paymentResultHoldsFields() {
        PaymentResult result = new PaymentResult("pi_1", "secret_1", PaymentResult.PaymentStatus.SUCCEEDED);
        assertThat(result.paymentIntentId()).isEqualTo("pi_1");
        assertThat(result.clientSecret()).isEqualTo("secret_1");
        assertThat(result.status()).isEqualTo(PaymentResult.PaymentStatus.SUCCEEDED);
    }

    @Test
    void refundResultHoldsFields() {
        RefundResult result = new RefundResult("re_1", "pi_1", 500L, RefundResult.RefundStatus.SUCCEEDED);
        assertThat(result.refundId()).isEqualTo("re_1");
        assertThat(result.paymentIntentId()).isEqualTo("pi_1");
        assertThat(result.amountInCents()).isEqualTo(500L);
        assertThat(result.status()).isEqualTo(RefundResult.RefundStatus.SUCCEEDED);
    }
}
