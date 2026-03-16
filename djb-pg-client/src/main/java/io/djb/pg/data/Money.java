package io.djb.pg.data;

import java.math.BigDecimal;

/**
 * A PostgreSQL money value. Stored as cents internally.
 */
public record Money(BigDecimal bigDecimalValue) {

    public static Money parse(String s) {
        s = s.trim();
        // Remove currency symbol and grouping separators
        s = s.replaceAll("[^0-9.\\-]", "");
        return new Money(new BigDecimal(s));
    }

    @Override
    public String toString() {
        return bigDecimalValue.toPlainString();
    }
}
