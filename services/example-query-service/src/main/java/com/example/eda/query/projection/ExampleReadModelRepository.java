package com.example.eda.query.projection;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ExampleReadModelRepository extends JpaRepository<ExampleReadModel, UUID> {
}
