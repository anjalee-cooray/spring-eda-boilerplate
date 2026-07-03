package com.example.eda.payments;

import com.example.eda.payments.stripe.StripeProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

@AutoConfiguration
@ComponentScan(basePackages = "com.example.eda.payments")
@EnableConfigurationProperties(StripeProperties.class)
public class PaymentsAutoConfiguration {
}
