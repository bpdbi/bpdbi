package io.djb.pg.data;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A PostgreSQL interval.
 */
public record Interval(int years, int months, int days, int hours, int minutes, int seconds, int microseconds) {

    public Interval() {
        this(0, 0, 0, 0, 0, 0, 0);
    }

    public static Interval of(int years, int months, int days, int hours, int minutes, int seconds, int microseconds) {
        return new Interval(years, months, days, hours, minutes, seconds, microseconds);
    }

    public static Interval of(int years, int months, int days, int hours, int minutes, int seconds) {
        return new Interval(years, months, days, hours, minutes, seconds, 0);
    }

    public static Interval of(int years, int months, int days) {
        return new Interval(years, months, days, 0, 0, 0, 0);
    }

    private static final Pattern PG_INTERVAL = Pattern.compile(
        "(?:(\\d+)\\s+years?)?\\s*" +
        "(?:(\\d+)\\s+mons?)?\\s*" +
        "(?:(-?\\d+)\\s+days?)?\\s*" +
        "(?:(-?)(\\d+):(\\d+):(\\d+)(?:\\.(\\d+))?)?");

    /**
     * Parse from PG text format, e.g. "1 year 2 mons 3 days 04:05:06.000007"
     */
    public static Interval parse(String s) {
        s = s.trim();
        Matcher m = PG_INTERVAL.matcher(s);
        if (!m.matches()) {
            throw new IllegalArgumentException("Cannot parse interval: " + s);
        }
        int y = m.group(1) != null ? Integer.parseInt(m.group(1)) : 0;
        int mo = m.group(2) != null ? Integer.parseInt(m.group(2)) : 0;
        int d = m.group(3) != null ? Integer.parseInt(m.group(3)) : 0;
        boolean neg = "-".equals(m.group(4));
        int h = m.group(5) != null ? Integer.parseInt(m.group(5)) : 0;
        int mi = m.group(6) != null ? Integer.parseInt(m.group(6)) : 0;
        int sec = m.group(7) != null ? Integer.parseInt(m.group(7)) : 0;
        int us = 0;
        if (m.group(8) != null) {
            String frac = m.group(8);
            // Pad to 6 digits
            while (frac.length() < 6) frac = frac + "0";
            us = Integer.parseInt(frac.substring(0, 6));
        }
        if (neg) { h = -h; mi = -mi; sec = -sec; us = -us; }
        return new Interval(y, mo, d, h, mi, sec, us);
    }

    public Duration toDuration() {
        long totalSeconds = ((((years * 12L + months) * 30L + days) * 24L + hours) * 60 + minutes) * 60 + seconds;
        return Duration.ofSeconds(totalSeconds).plusNanos(microseconds * 1000L);
    }

    @Override
    public String toString() {
        return "Interval(" + years + " years " + months + " months " + days + " days "
            + hours + " hours " + minutes + " minutes " + seconds + " seconds "
            + microseconds + " microseconds)";
    }
}
