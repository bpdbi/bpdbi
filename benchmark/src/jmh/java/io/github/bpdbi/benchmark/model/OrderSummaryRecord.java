package io.github.bpdbi.benchmark.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record OrderSummaryRecord(
    int id,
    BigDecimal total,
    String status,
    LocalDateTime createdAt,
    String username,
    long itemCount) {}
