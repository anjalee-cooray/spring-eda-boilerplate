package com.example.eda.query.api;

import com.example.eda.query.projection.ExampleReadModel;
import com.example.eda.query.projection.ExampleReadModelRepository;
import com.example.eda.security.RequiredScope;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/queries/examples")
@PreAuthorize("hasAnyRole('TENANT_ADMIN', 'TENANT_MEMBER')")
@RequiredScope("queries:read")
public class ExampleQueryController {

    private final ExampleReadModelRepository repository;

    public ExampleQueryController(ExampleReadModelRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public ResponseEntity<List<ExampleReadModel>> findAll() {
        return ResponseEntity.ok(repository.findAll());
    }

    @GetMapping("/{id}")
    public ResponseEntity<ExampleReadModel> findById(@PathVariable UUID id) {
        return repository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
