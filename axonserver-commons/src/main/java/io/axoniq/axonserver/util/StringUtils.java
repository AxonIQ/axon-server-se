/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

/**
 * @author Marc Gathier
 */
public class StringUtils {

    private StringUtils() {
    }

    public static String getOrDefault(String value, String defaultValue) {
        return isEmpty(value) ? defaultValue : value;
    }

    public static boolean isEmpty(String value) {
        return value == null || "".equals(value);
    }

    /**
     * Sanitizes a user provided parameter for logging.
     *
     * @param string the input string
     * @return the sanitized string
     */
    public static String sanitize(String string) {
        if (string == null) {
            return null;
        }
        return string.replaceAll("[\n|\r|\t]", "_");
    }

    public static String username(String username) {
        return sanitize(getOrDefault(username, "<anonymous>"));
    }
}
