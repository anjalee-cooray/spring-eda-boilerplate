package com.example.eda.command.api;

import jakarta.validation.constraints.NotBlank;

public record CreateExampleCommand(
        @NotBlank String name
) {
}
