package com.example.eda.command.api;

import com.example.eda.db.quota.QuotaExceededException;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/commands/examples")
@PreAuthorize("hasAnyRole('TENANT_ADMIN', 'TENANT_MEMBER')")
public class ExampleCommandController {

    private final ExampleCommandHandler handler;

    public ExampleCommandController(ExampleCommandHandler handler) {
        this.handler = handler;
    }

    @PostMapping
    public ResponseEntity<CreateExampleResponse> create(
            @Valid @RequestBody CreateExampleCommand command,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {

        UUID id = handler.handle(command, correlationId != null ? correlationId : UUID.randomUUID().toString());
        return ResponseEntity.status(HttpStatus.CREATED).body(new CreateExampleResponse(id));
    }

    @ExceptionHandler(QuotaExceededException.class)
    public ResponseEntity<QuotaErrorResponse> handleQuotaExceeded(QuotaExceededException ex) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(new QuotaErrorResponse(
                        "quota_exceeded",
                        "Daily write quota exhausted. Upgrade your plan or retry tomorrow.",
                        ex.getCurrent(),
                        ex.getLimit()));
    }

    record CreateExampleResponse(UUID id) { }
    record QuotaErrorResponse(String error, String message, long quotaUsed, long quotaLimit) { }
}
