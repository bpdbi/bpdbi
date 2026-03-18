package io.github.bpdbi.benchmark.model;

import java.time.LocalDateTime;

public record UserRecord(
    int id,
    String username,
    String email,
    String fullName,
    String bio,
    boolean active,
    LocalDateTime createdAt) {}
