/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.admin.user.api.UserAdminService;
import io.axoniq.axonserver.admin.user.requestprocessor.UserController;
import io.axoniq.axonserver.applicationevents.UserEvents;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Collections;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class AxonServerStandardConfigurationTest {
    private AxonServerStandardConfiguration testSubject = new AxonServerStandardConfiguration();

    @Test
    public void deleteUserRaisesEvent() {

        ApplicationEventPublisher applicationEventPublisher = mock(ApplicationEventPublisher.class);
        UserController userController = mock(UserController.class);
        UserAdminService facade = testSubject.userAdminService(userController,
                                                                       applicationEventPublisher);
        facade.deleteUser("User");
        verify(applicationEventPublisher).publishEvent(argThat(new ArgumentMatcher<Object>() {
            @Override
            public boolean matches(Object o) {
                return o instanceof UserEvents.UserDeleted && ((UserEvents.UserDeleted) o).getName().equals("User");
            }
        }));
    }
    @Test
    public void updateUserRaisesEvent() {

        ApplicationEventPublisher applicationEventPublisher = mock(ApplicationEventPublisher.class);
        UserController userController = mock(UserController.class);
        doAnswer((invocationOnMock ->
                new User((String)invocationOnMock.getArguments()[0], (String)invocationOnMock.getArguments()[1])
                 )).when(userController).updateUser(any(), any(), any());
        UserAdminService facade = testSubject.userAdminService(userController,
                                                                       applicationEventPublisher);
        facade.createOrUpdateUser("User", "Password", Collections.emptySet());
        verify(applicationEventPublisher).publishEvent(argThat(new ArgumentMatcher<Object>() {
            @Override
            public boolean matches(Object o) {
                return o instanceof UserEvents.UserUpdated&& ((UserEvents.UserUpdated) o).getUser().getUserName().equals("User");
            }
        }));
    }

}