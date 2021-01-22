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
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.InvalidTokenException;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 * @since 4.0
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
    public Authentication authentication(String context, String token) {
        if (!isTokenFromConfigFile(token)) {
            throw new InvalidTokenException();
        }
        return isAdminToken(token) ?
                GrpcContextAuthenticationProvider.ADMIN_PRINCIPAL :
                GrpcContextAuthenticationProvider.USER_PRINCIPAL;
    }

    private boolean isAdminToken(String token) {
        return (token != null) && token.equals(messagingPlatformConfiguration.getAccesscontrol().getAdminToken());
    }

    private boolean isTokenFromConfigFile(String token) {
        AccessControlConfiguration config = messagingPlatformConfiguration.getAccesscontrol();

        return (token != null) && (token.equals(config.getToken()) || token.equals(config.getAdminToken()));
    }
}
