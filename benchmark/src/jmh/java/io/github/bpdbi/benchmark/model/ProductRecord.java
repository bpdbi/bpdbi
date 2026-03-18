package io.github.bpdbi.benchmark.model;

import java.math.BigDecimal;

public record ProductRecord(
    int id, String name, String description, BigDecimal price, String category, int stock) {}
