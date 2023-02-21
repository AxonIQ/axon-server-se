/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import io.axoniq.axonserver.exception.InvalidTokenException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.core.Authentication;

import static org.junit.jupiter.api.Assertions.assertTrue;
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
        Authentication authentication = testSubject.authenticate("1");
        assertTrue(testSubject.allowed("/v1/commands", "default", authentication));
    }

    @Test(expected = InvalidTokenException.class)
    public void notAllowed() {
        testSubject.authenticate("2");
    }

}
