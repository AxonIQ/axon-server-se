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
    public static String getOrDefault(String value, String defaultValue) {
        return org.springframework.util.StringUtils.isEmpty(value) ? defaultValue : value;
    }

    public static boolean isEmpty(String value) {
        return org.springframework.util.StringUtils.isEmpty(value);
    }
}
