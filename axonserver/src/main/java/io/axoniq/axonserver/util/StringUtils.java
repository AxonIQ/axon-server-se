package io.axoniq.axonserver.util;

/**
 * Author: marc
 */
public class StringUtils {
    public static String getOrDefault(String value, String defaultValue) {
        return org.springframework.util.StringUtils.isEmpty(value) ? defaultValue : value;
    }

}
