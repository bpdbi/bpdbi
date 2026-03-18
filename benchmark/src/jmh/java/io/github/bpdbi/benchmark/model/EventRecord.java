package io.github.bpdbi.benchmark.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

public record EventRecord(
    int id,
    UUID eventUuid,
    String eventType,
    int userId,
    long sequenceNum,
    BigDecimal amount,
    BigDecimal discount,
    boolean processed,
    boolean flagged,
    String source,
    String category,
    @Nullable String notes,
    @Nullable String referenceCode,
    UUID correlationId,
    LocalDateTime createdAt,
    LocalDateTime updatedAt) {}
