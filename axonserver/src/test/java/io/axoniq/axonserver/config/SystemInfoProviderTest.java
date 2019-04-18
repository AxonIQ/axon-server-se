/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class SystemInfoProviderTest {
    private SystemInfoProvider testSubject = new SystemInfoProvider() {};
    @Test
    public void javaWithModulesOnWindows() {
        assertFalse(testSubject.javaOnWindows() && testSubject.javaWithModules());
    }
}