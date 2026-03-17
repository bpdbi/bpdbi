package io.github.bpdbi.pg.data;

import java.time.Duration;
import org.jspecify.annotations.NonNull;

/** A Postgres interval. */
public record Interval(
    int years, int months, int days, int hours, int minutes, int seconds, int microseconds) {

  public static @NonNull Interval of(
      int years, int months, int days, int hours, int minutes, int seconds, int microseconds) {
    return new Interval(years, months, days, hours, minutes, seconds, microseconds);
  }

  public static @NonNull Interval of(
      int years, int months, int days, int hours, int minutes, int seconds) {
    return new Interval(years, months, days, hours, minutes, seconds, 0);
  }

  public static @NonNull Interval of(int years, int months, int days) {
    return new Interval(years, months, days, 0, 0, 0, 0);
  }

  public @NonNull Duration toDuration() {
    long totalSeconds =
        ((((years * 12L + months) * 30L + days) * 24L + hours) * 60 + minutes) * 60 + seconds;
    return Duration.ofSeconds(totalSeconds).plusNanos(microseconds * 1000L);
  }

  @Override
  public @NonNull String toString() {
    return "Interval("
        + years
        + " years "
        + months
        + " months "
        + days
        + " days "
        + hours
        + " hours "
        + minutes
        + " minutes "
        + seconds
        + " seconds "
        + microseconds
        + " microseconds)";
  }
}
