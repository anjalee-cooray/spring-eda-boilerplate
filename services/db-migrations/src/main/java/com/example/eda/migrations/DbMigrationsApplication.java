package com.example.eda.migrations;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Short-lived Flyway migration runner.
 * Exits 0 on success, non-zero on failure.
 * Deploy as a Kubernetes Job or ECS Task before rolling out any service.
 */
@SpringBootApplication
public class DbMigrationsApplication {

    public static void main(String[] args) {
        SpringApplication.run(DbMigrationsApplication.class, args);
    }
}
