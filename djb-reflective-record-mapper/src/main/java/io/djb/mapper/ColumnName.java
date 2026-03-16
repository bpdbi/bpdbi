package io.djb.mapper;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Overrides the column name used when mapping a record component from a {@link io.djb.Row}.
 *
 * <p>By default, {@link ReflectiveRecordMapper} matches record component names to column names
 * exactly. Use this annotation when the column name differs from the component name.
 *
 * <pre>{@code
 * record User(
 *     int id,
 *     @ColumnName("first_name") String firstName,
 *     @ColumnName("last_name") String lastName
 * ) {}
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface ColumnName {
    String value();
}
