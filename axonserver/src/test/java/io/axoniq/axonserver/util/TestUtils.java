/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.mock.http.MockHttpOutputMessage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Zoltan Altfatter
 */
public class TestUtils {

    @SuppressWarnings("unchecked")
    public static String toJson(Object object, HttpMessageConverter converter) throws IOException {
        MockHttpOutputMessage mockHttpOutputMessage = new MockHttpOutputMessage();
        converter.write(object, MediaType.APPLICATION_JSON, mockHttpOutputMessage);
        return mockHttpOutputMessage.getBodyAsString();
    }

    public static String fixPathOnWindows(String file) {
        if( file.contains(":") && file.startsWith("/")) return file.substring(1);
        return file;
    }

    public static void clearDirectory(Path storageFolder) throws IOException {
        if( Files.exists(storageFolder) ) {
            Files.list(storageFolder).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } else {
            Files.createDirectories(storageFolder);
        }
    }

}
