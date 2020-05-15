package io.axoniq.axonserver.util;

/**
 * Contains some general utility functions.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class ObjectUtils {

    /**
     * Checks given value and if null, returns a default value.
     *
     * @param value        the value to return
     * @param defaultValue the value to return if {@code value} is null
     * @param <T>          the type of object
     * @return {@code value}  if not null, {@code defaultValue} otherwise.
     */
    public static <T> T getOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }
}
