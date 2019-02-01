package io.axoniq.axonserver.util;

/**
 * @author Marc Gathier
 */
public class StringUtils {
    public static String getOrDefault(String value, String defaultValue) {
        return org.springframework.util.StringUtils.isEmpty(value) ? defaultValue : value;
    }

    public static boolean isEmpty(String value) {
        return org.springframework.util.StringUtils.isEmpty(value);
    }
}
