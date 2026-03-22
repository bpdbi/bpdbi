package io.github.bpdbi.pg.data;

import java.util.List;
import org.jspecify.annotations.NonNull;

/** Postgres 'polygon': ((x1,y1),(x2,y2),...). */
public record Polygon(@NonNull List<Point> points) {}
