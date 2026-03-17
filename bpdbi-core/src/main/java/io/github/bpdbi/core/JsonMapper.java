package io.github.bpdbi.core;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Pluggable JSON serialization/deserialization interface. Users provide their own implementation
 * backed by Jackson, Gson, Moshi, etc.
 *
 * <pre>{@code
 * conn.setJsonMapper(new JsonMapper() {
 *     private final ObjectMapper om = new ObjectMapper();
 *     public <T> T fromJson(String json, Class<T> type) { return om.readValue(json, type); }
 *     public String toJson(Object value) { return om.writeValueAsString(value); }
 * });
 * }</pre>
 */
public interface JsonMapper {

  @Nullable <T> T fromJson(@NonNull String json, @NonNull Class<T> type);

  @NonNull String toJson(@NonNull Object value);
}
