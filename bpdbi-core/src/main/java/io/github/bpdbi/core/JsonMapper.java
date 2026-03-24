package io.github.bpdbi.core;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Pluggable JSON serialization/deserialization interface. Implement this with your preferred JSON
 * library (Jackson, Gson, Moshi, kotlinx.serialization, etc.) and set it on the connection via
 * {@link Connection#setJsonMapper(JsonMapper)}.
 *
 * <p>Once configured, JSON/JSONB columns are automatically deserialized via {@link Row#get(int,
 * Class)}, and types registered with {@link TypeRegistry#registerAsJson(Class)} are serialized when
 * used as query parameters.
 *
 * <p><b>Jackson example:</b>
 *
 * <pre>{@code
 * var om = new ObjectMapper();
 * conn.setJsonMapper(new JsonMapper() {
 *     public <T> T fromJson(String json, Class<T> type) {
 *         try { return om.readValue(json, type); }
 *         catch (JsonProcessingException e) { throw new RuntimeException(e); }
 *     }
 *     public String toJson(Object value) {
 *         try { return om.writeValueAsString(value); }
 *         catch (JsonProcessingException e) { throw new RuntimeException(e); }
 *     }
 * });
 * }</pre>
 *
 * <p><b>Gson example:</b>
 *
 * <pre>{@code
 * var gson = new Gson();
 * conn.setJsonMapper(new JsonMapper() {
 *     public <T> T fromJson(String json, Class<T> type) { return gson.fromJson(json, type); }
 *     public String toJson(Object value) { return gson.toJson(value); }
 * });
 * }</pre>
 *
 * <p>Implementations should throw an unchecked exception (e.g., wrapping the library's checked
 * exception) if serialization or deserialization fails. The exception will propagate to the caller
 * of {@link Row#get} or {@link Connection#query}.
 */
public interface JsonMapper {

  /**
   * Deserialize a JSON string to the given type. Return {@code null} if the JSON represents a null
   * value.
   *
   * @param json the JSON string (never null)
   * @param type the target type
   * @param <T> the target type
   * @return the deserialized object, or null
   */
  @Nullable <T> T fromJson(@NonNull String json, @NonNull Class<T> type);

  /**
   * Serialize an object to a JSON string.
   *
   * @param value the object to serialize (never null)
   * @return the JSON string
   */
  @NonNull String toJson(@NonNull Object value);
}
