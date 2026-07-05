package com.example.eda.command.api;

import com.example.eda.command.domain.ExampleEntity;
import com.example.eda.command.domain.ExampleRepository;
import com.example.eda.db.outbox.OutboxWriter;
import com.example.eda.db.quota.TenantQuotaEnforcer;
import com.example.eda.security.TenantContextHolder;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ExampleCommandHandler {

    private final ExampleRepository repository;
    private final OutboxWriter outboxWriter;
    private final TenantQuotaEnforcer quotaEnforcer;

    public ExampleCommandHandler(
            ExampleRepository repository,
            OutboxWriter outboxWriter,
            TenantQuotaEnforcer quotaEnforcer) {
        this.repository   = repository;
        this.outboxWriter = outboxWriter;
        this.quotaEnforcer = quotaEnforcer;
    }

    @Transactional
    public UUID handle(CreateExampleCommand command, String correlationId) {
        String tenantId = TenantContextHolder.get().tenantId();

        // Domain-level quota check — secondary defence for callers that bypass the
        // API gateway. Throws QuotaExceededException (→ 429) if daily limit exceeded.
        quotaEnforcer.enforceWriteQuota(tenantId);

        ExampleEntity entity = ExampleEntity.builder()
                .tenantId(tenantId)
                .name(command.name())
                .build();

        ExampleEntity saved = repository.save(entity);

        outboxWriter.write(
                "example.created",
                new ExampleCreatedPayload(saved.getId(), saved.getName(), tenantId),
                correlationId
        );

        return saved.getId();
    }

    record ExampleCreatedPayload(UUID id, String name, String tenantId) { }
}
