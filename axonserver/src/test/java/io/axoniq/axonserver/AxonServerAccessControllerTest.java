/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.admin.user.requestprocessor.UserController;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class AxonServerAccessControllerTest {
    private AxonServerAccessController testSubject;

    @Mock
    private MessagingPlatformConfiguration messagingPlatformConfiguration;

    @Before
    public void setup() {
        UserController userController = mock(UserController.class);
        testSubject = new AxonServerStandardAccessController(messagingPlatformConfiguration, userController);
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
