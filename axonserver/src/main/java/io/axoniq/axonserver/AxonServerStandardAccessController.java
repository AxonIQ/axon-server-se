/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.InvalidTokenException;
import io.axoniq.axonserver.config.DefaultAuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.util.Set;

import static io.axoniq.axonserver.rest.json.UserInfo.ROLE_ADMIN;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * Created by marc on 7/17/2017.
 */
@Component
public class AxonServerStandardAccessController implements AxonServerAccessController {

    private final MessagingPlatformConfiguration messagingPlatformConfiguration;

    public AxonServerStandardAccessController(MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @Override
    public boolean allowed(String fullMethodName, String context, String token) {
        return isTokenFromConfigFile(token);
    }

    @Override
    public boolean isRoleBasedAuthentication() {
        return false;
    }

    @Override
    public Set<String> getRoles(String token) {

        if (!isTokenFromConfigFile(token)) {
            throw new InvalidTokenException();
        }
        return isAdminToken(token) ? singleton(ROLE_ADMIN) : emptySet();
    }

    @Override
    public Authentication authentication(String token) {
        return isAdminToken(token) ?
                DefaultAuthenticationProvider.ADMIN_PRINCIPAL :
                DefaultAuthenticationProvider.USER_PRINCIPAL;
    }

    private boolean isAdminToken(String token) {
        return (token != null) && token.equals(messagingPlatformConfiguration.getAccesscontrol().getAdminToken());
    }

    private boolean isTokenFromConfigFile(String token) {
        AccessControlConfiguration config = messagingPlatformConfiguration.getAccesscontrol();

        return (token != null) && (token.equals(config.getToken()) || token.equals(config.getAdminToken()));
    }
}
