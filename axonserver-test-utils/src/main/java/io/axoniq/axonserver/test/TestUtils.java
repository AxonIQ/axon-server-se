/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Zoltan Altfatter
 * @since 4.0
 */
public class TestUtils {

    public static String fixPathOnWindows(String file) {
        if (file.contains(":") && file.startsWith("/")) {
            return file.substring(1);
        }
        return file;
    }
}
