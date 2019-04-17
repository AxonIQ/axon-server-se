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

import java.net.UnknownHostException;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class MessagingPlatformConfigurationTest {
    private MessagingPlatformConfiguration testSubject;

    @Test
    public void getHostname() {
        testSubject = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public int getPort() {
                return 0;
            }

            @Override
            public String getHostName() throws UnknownHostException {
                return "test.axoniq.io";
            }
        });
        assertEquals("test.axoniq.io", testSubject.getHostname());
    }

    @Test
    public void getHostnameWithDomainSet() {
        testSubject = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public int getPort() {
                return 0;
            }

            @Override
            public String getHostName() throws UnknownHostException {
                return "test.axoniq.io";
            }
        });
        testSubject.setDomain("axoniq.io");
        assertEquals("test", testSubject.getHostname());
        assertEquals("test.axoniq.io", testSubject.getFullyQualifiedHostname());
    }

    @Test
    public void getHostnameWithBlankDomain() {
        testSubject = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public int getPort() {
                return 0;
            }

            @Override
            public String getHostName() throws UnknownHostException {
                return "test";
            }
        });
        testSubject.setDomain("");
        assertEquals("test", testSubject.getFullyQualifiedHostname());
        assertEquals("test", testSubject.getFullyQualifiedInternalHostname());
    }
}
