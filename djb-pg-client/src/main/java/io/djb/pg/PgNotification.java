package io.djb.pg;

import org.jspecify.annotations.NonNull;

/**
 * A Postgres asynchronous notification received via LISTEN/NOTIFY.
 *
 * @param processId the backend process ID that sent the notification
 * @param channel the notification channel name
 * @param payload the notification payload (empty string if no payload)
 */
public record PgNotification(int processId, @NonNull String channel, @NonNull String payload) {
  // Intentionally empty
}
