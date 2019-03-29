/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.access.pathmapping.PathMappingRepository;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class AxonServerAccessControllerTest {
    private AxonServerAccessController testSubject;

    @Mock
    private MessagingPlatformConfiguration messagingPlatformConfiguration;
    @Mock
    private PathMappingRepository pathMappingRepository;

    @Before
    public void setup() {
        testSubject = new AxonServerStandardAccessController( pathMappingRepository, messagingPlatformConfiguration);
        AccessControlConfiguration accessControlConfiguation = new AccessControlConfiguration();
        accessControlConfiguation.setToken("1");
        when(messagingPlatformConfiguration.getAccesscontrol()).thenReturn(accessControlConfiguation);
    }
    @Test
    public void allowed() {
        assertTrue(testSubject.allowed("/v1/commands", "default", "1"));
    }

    @Test
    public void notAllowed() {
        assertFalse(testSubject.allowed("/v1/commands", "default", "2"));
    }

}
