package com.example.eda.gateway;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Extracts tenant_id and correlation_id from the validated JWT and propagates
 * them as request headers to downstream services.
 */
@Component
public class TenantContextGatewayFilter implements GlobalFilter, Ordered {

    static final String TENANT_ID_HEADER = "X-Tenant-Id";
    static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    static final String TENANT_ID_CLAIM = "tenant_id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        return ReactiveSecurityContextHolder.getContext()
                .flatMap(ctx -> {
                    if (ctx.getAuthentication() != null
                            && ctx.getAuthentication().getPrincipal() instanceof Jwt jwt) {
                        String tenantId = jwt.getClaimAsString(TENANT_ID_CLAIM);
                        String correlationId = exchange.getRequest().getId();

                        ServerHttpRequest mutated = exchange.getRequest().mutate()
                                .header(TENANT_ID_HEADER, tenantId != null ? tenantId : "")
                                .header(CORRELATION_ID_HEADER, correlationId)
                                .build();

                        return chain.filter(exchange.mutate().request(mutated).build());
                    }
                    return chain.filter(exchange);
                })
                .switchIfEmpty(chain.filter(exchange));
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 1;
    }
}
