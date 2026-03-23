package io.github.bpdbi.core.test;

import io.github.bpdbi.core.Connection;
import io.github.bpdbi.core.Cursor;
import io.github.bpdbi.core.JsonMapper;
import io.github.bpdbi.core.PreparedStatement;
import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowSet;
import io.github.bpdbi.core.RowStream;
import io.github.bpdbi.core.TypeRegistry;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Base class for Connection stubs in tests. All methods return safe defaults (null, empty
 * collections, or throw UnsupportedOperationException). Override only what your test needs.
 */
public abstract class AbstractStubConnection implements Connection {

  @Override
  public @NonNull RowSet query(@NonNull String sql) {
    return new RowSet(List.of(), List.of(), 0);
  }

  @Override
  public @NonNull RowSet query(@NonNull String sql, Object... params) {
    return new RowSet(List.of(), List.of(), 0);
  }

  @Override
  public @NonNull RowSet query(@NonNull String sql, @NonNull Map<String, Object> params) {
    return new RowSet(List.of(), List.of(), 0);
  }

  @Override
  public int enqueue(@NonNull String sql) {
    return 0;
  }

  @Override
  public int enqueue(@NonNull String sql, Object... params) {
    return 0;
  }

  @Override
  public int enqueue(@NonNull String sql, @NonNull Map<String, Object> params) {
    return 0;
  }

  @Override
  public @NonNull List<RowSet> flush() {
    return List.of();
  }

  @Override
  public @NonNull List<RowSet> executeMany(@NonNull String sql, @NonNull List<Object[]> paramSets) {
    return List.of();
  }

  @Override
  public @NonNull List<RowSet> executeManyNamed(
      @NonNull String sql, @NonNull List<Map<String, Object>> paramSets) {
    return List.of();
  }

  @Override
  public @NonNull PreparedStatement prepare(@NonNull String sql) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull Cursor cursor(@NonNull String sql, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void ping() {}

  @Override
  public void queryStream(@NonNull String sql, @NonNull Consumer<Row> consumer) {}

  @Override
  public void queryStream(@NonNull String sql, Object[] params, @NonNull Consumer<Row> consumer) {}

  @Override
  public @NonNull RowStream stream(@NonNull String sql, Object... params) {
    return new RowStream(() -> null, () -> {});
  }

  @Override
  public @NonNull Map<String, String> parameters() {
    return Map.of();
  }

  @Override
  public @NonNull TypeRegistry typeRegistry() {
    return new TypeRegistry();
  }

  @Override
  public void setTypeRegistry(@NonNull TypeRegistry registry) {}

  @Override
  public @Nullable JsonMapper jsonMapper() {
    return null;
  }

  @Override
  public void setJsonMapper(@Nullable JsonMapper mapper) {}

  @Override
  public void close() {}
}
