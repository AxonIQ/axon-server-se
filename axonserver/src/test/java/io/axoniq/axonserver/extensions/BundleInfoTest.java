/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.*;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class BundleInfoTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void version() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        String serialized = objectMapper.writeValueAsString(new BundleInfo("name", "version"));
        System.out.println(serialized);
        BundleInfo deserialized = objectMapper.readValue(serialized, BundleInfo.class);
        assertEquals("name", deserialized.getSymbolicName());
        assertEquals("version", deserialized.getVersion());
    }
}