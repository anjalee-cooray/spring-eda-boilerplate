package com.example.eda.db;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@AutoConfiguration
@EntityScan(basePackages = "com.example.eda.db")
@EnableJpaRepositories(basePackages = "com.example.eda.db")
@ComponentScan(basePackages = "com.example.eda.db")
public class DbAutoConfiguration {
}
