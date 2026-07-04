package com.example.eda.gateway.idempotency;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Enforces idempotency on mutating HTTP requests (POST, PUT, DELETE) to command routes.
 *
 * Flow:
 *   1. Client sends Idempotency-Key header with every mutating request
 *   2. Gateway checks Redis for an existing response under that key
 *      HIT  → return cached response immediately (command-service not called)
 *      MISS → set "processing" marker to block concurrent duplicates
 *           → forward to command-service
 *           → cache response in Redis with TTL=24h
 *           → return response to client
 *
 * Cache key: "idempotency:{tenant_id}:{idempotency_key}"
 *   Scoped by tenant_id so keys from different tenants never collide.
 *
 * Missing Idempotency-Key header on a mutating request returns 400.
 * GET requests are skipped — they are inherently idempotent.
 */
@Component
public class IdempotencyFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyFilter.class);

    static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    static final String IDEMPOTENCY_STATUS_HEADER = "Idempotency-Status";
    private static final String CACHE_PREFIX = "idempotency";
    private static final String PROCESSING_SUFFIX = ":processing";
    private static final Duration CACHE_TTL = Duration.ofHours(24);
    private static final Duration PROCESSING_TTL = Duration.ofSeconds(30);

    private static final Set<HttpMethod> MUTATING_METHODS =
            Set.of(HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH, HttpMethod.DELETE);

    private static final String COMMAND_PATH_PREFIX = "/api/commands";

    private final ReactiveStringRedisTemplate redis;

    public IdempotencyFilter(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public int getOrder() {
        // Run after TenantContextGatewayFilter (HIGHEST_PRECEDENCE + 1)
        // so tenant_id is already available in headers
        return Ordered.HIGHEST_PRECEDENCE + 2;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        HttpMethod method = exchange.getRequest().getMethod();
        String path = exchange.getRequest().getPath().value();

        // Only apply to mutating requests on command routes
        if (!MUTATING_METHODS.contains(method) || !path.startsWith(COMMAND_PATH_PREFIX)) {
            return chain.filter(exchange);
        }

        String idempotencyKey = exchange.getRequest().getHeaders().getFirst(IDEMPOTENCY_KEY_HEADER);
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return rejectMissingKey(exchange);
        }

        String tenantId = exchange.getRequest().getHeaders().getFirst("X-Tenant-Id");
        String cacheKey = buildCacheKey(tenantId, idempotencyKey);
        String processingKey = cacheKey + PROCESSING_SUFFIX;

        return redis.opsForValue().get(cacheKey)
                .flatMap(cached -> returnCachedResponse(exchange, cached, idempotencyKey))
                .switchIfEmpty(processAndCache(exchange, chain, cacheKey, processingKey, idempotencyKey));
    }

    private Mono<Void> returnCachedResponse(
            ServerWebExchange exchange, String cached, String idempotencyKey) {
        log.info("Idempotency cache HIT — returning cached response key={}", idempotencyKey);

        IdempotencyCachedResponse cachedResponse = IdempotencyCachedResponse.deserialize(cached);
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.valueOf(cachedResponse.statusCode()));
        response.getHeaders().add(IDEMPOTENCY_STATUS_HEADER, "HIT");
        response.getHeaders().add("Content-Type", "application/json");

        byte[] bytes = cachedResponse.body().getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }

    private Mono<Void> processAndCache(
            ServerWebExchange exchange,
            GatewayFilterChain chain,
            String cacheKey,
            String processingKey,
            String idempotencyKey) {

        return redis.opsForValue()
                // SETNX — only set if not exists; returns true if we got the lock
                .setIfAbsent(processingKey, "1", PROCESSING_TTL)
                .flatMap(acquired -> {
                    if (Boolean.FALSE.equals(acquired)) {
                        // Another request with the same key is in flight
                        log.warn("Idempotency key in flight — concurrent duplicate rejected key={}", idempotencyKey);
                        return rejectConcurrentDuplicate(exchange);
                    }

                    log.debug("Idempotency cache MISS — forwarding request key={}", idempotencyKey);
                    exchange.getResponse().getHeaders().add(IDEMPOTENCY_STATUS_HEADER, "MISS");

                    CapturingResponseDecorator capturingResponse =
                            new CapturingResponseDecorator(exchange.getResponse());
                    ServerWebExchange mutatedExchange = exchange.mutate()
                            .response(capturingResponse)
                            .build();

                    return chain.filter(mutatedExchange)
                            .then(Mono.defer(() -> cacheResponse(cacheKey, capturingResponse, idempotencyKey)))
                            .then(redis.delete(processingKey).then());
                });
    }

    private Mono<Void> cacheResponse(
            String cacheKey,
            CapturingResponseDecorator capturingResponse,
            String idempotencyKey) {
        int statusCode = capturingResponse.getStatusCode() != null
                ? capturingResponse.getStatusCode().value()
                : 200;
        String body = capturingResponse.getCapturedBody();

        // Only cache successful responses — don't cache 5xx errors
        if (statusCode >= 500) {
            log.warn("Idempotency: not caching 5xx response key={} status={}", idempotencyKey, statusCode);
            return Mono.empty();
        }

        IdempotencyCachedResponse toCache = new IdempotencyCachedResponse(statusCode, body);
        return redis.opsForValue()
                .set(cacheKey, toCache.serialize(), CACHE_TTL)
                .doOnSuccess(v -> log.debug("Idempotency response cached key={} status={} ttl=24h",
                        idempotencyKey, statusCode))
                .then();
    }

    private Mono<Void> rejectMissingKey(ServerWebExchange exchange) {
        log.warn("Mutating request missing Idempotency-Key header path={}",
                exchange.getRequest().getPath());
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.BAD_REQUEST);
        response.getHeaders().add("Content-Type", "application/json");
        byte[] bytes = """
                {"error":"missing_idempotency_key",\
                "message":"Idempotency-Key header is required for POST, PUT, PATCH and DELETE requests."}\
                """.getBytes(StandardCharsets.UTF_8);
        return response.writeWith(Mono.just(response.bufferFactory().wrap(bytes)));
    }

    private Mono<Void> rejectConcurrentDuplicate(ServerWebExchange exchange) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.CONFLICT);
        response.getHeaders().add("Content-Type", "application/json");
        byte[] bytes = """
                {"error":"request_in_flight",\
                "message":"A request with this Idempotency-Key is already being processed. Retry in a moment."}\
                """.getBytes(StandardCharsets.UTF_8);
        return response.writeWith(Mono.just(response.bufferFactory().wrap(bytes)));
    }

    private String buildCacheKey(String tenantId, String idempotencyKey) {
        String tenant = (tenantId != null && !tenantId.isBlank()) ? tenantId : "unknown";
        return CACHE_PREFIX + ":" + tenant + ":" + idempotencyKey;
    }

    /**
     * Decorates the response to capture the body so it can be cached in Redis.
     * The response still writes through to the client normally.
     */
    static class CapturingResponseDecorator extends ServerHttpResponseDecorator {

        private final StringBuilder bodyCapture = new StringBuilder();

        CapturingResponseDecorator(ServerHttpResponse delegate) {
            super(delegate);
        }

        @Override
        public Mono<Void> writeWith(org.reactivestreams.Publisher<? extends DataBuffer> body) {
            return super.writeWith(
                    Flux.from(body).doOnNext(buffer -> {
                        byte[] bytes = new byte[buffer.readableByteCount()];
                        buffer.toByteBuffer().get(bytes);
                        bodyCapture.append(new String(bytes, StandardCharsets.UTF_8));
                    })
            );
        }

        String getCapturedBody() {
            return bodyCapture.toString();
        }
    }
}
