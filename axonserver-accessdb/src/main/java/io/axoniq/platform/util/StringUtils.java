package io.axoniq.platform.util;

/**
 * @author Marc Gathier
 */
public class StringUtils {
    public static String getOrDefault(String value, String defaultValue) {
        return org.springframework.util.StringUtils.isEmpty(value) ? defaultValue : value;
    }

}
