package io.github.bpdbi.pg.data;

import java.util.List;
import org.jspecify.annotations.NonNull;

/** Postgres 'path'. Can be open [(x1,y1),...] or closed ((x1,y1),...). */
public record Path(boolean isOpen, @NonNull List<Point> points) {}
