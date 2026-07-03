package com.example.eda.command.domain;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ExampleRepository extends JpaRepository<ExampleEntity, UUID> {
}
