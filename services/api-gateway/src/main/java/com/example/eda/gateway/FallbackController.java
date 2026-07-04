package com.example.eda.gateway;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Returns structured 503 responses when a circuit breaker is OPEN.
 * The client receives a clear error rather than waiting for a timeout.
 *
 * Circuit breaker states:
 *   CLOSED    → requests flow normally to the downstream service
 *   OPEN      → requests fail immediately here — downstream not called
 *   HALF-OPEN → one test request allowed through to check recovery
 */
@RestController
@RequestMapping("/fallback")
public class FallbackController {

    private static final Logger log = LoggerFactory.getLogger(FallbackController.class);

    @PostMapping("/command-service")
    @GetMapping("/command-service")
    public ResponseEntity<Map<String, String>> commandServiceFallback() {
        log.warn("Circuit breaker OPEN — command-service unavailable, returning 503");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "30")
                .body(Map.of(
                        "error", "service_unavailable",
                        "message", "Command service is temporarily unavailable. Please retry in 30 seconds.",
                        "retryAfterSeconds", "30"
                ));
    }

    @GetMapping("/query-service")
    public ResponseEntity<Map<String, String>> queryServiceFallback() {
        log.warn("Circuit breaker OPEN — query-service unavailable, returning 503");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "10")
                .body(Map.of(
                        "error", "service_unavailable",
                        "message", "Query service is temporarily unavailable. Please retry in 10 seconds.",
                        "retryAfterSeconds", "10"
                ));
    }
}
