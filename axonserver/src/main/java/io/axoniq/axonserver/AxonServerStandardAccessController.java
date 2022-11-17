/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.admin.user.requestprocessor.UserController;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.InvalidTokenException;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;

import java.util.Collections;
import java.util.Set;
import javax.annotation.PostConstruct;

/**
 * @author Marc Gathier
 * @since 4.0
 */
public class AxonServerStandardAccessController implements AxonServerAccessController {

    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final UserController userController;

    public AxonServerStandardAccessController(MessagingPlatformConfiguration messagingPlatformConfiguration,
                                              UserController userController) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.userController = userController;
    }

    @PostConstruct
    public void validate() {
        if (messagingPlatformConfiguration.getAccesscontrol() == null ||
                !messagingPlatformConfiguration.getAccesscontrol().isEnabled()) {
            return;
        }
        if (messagingPlatformConfiguration.getAccesscontrol().getAdminToken() == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "Missing required admin token (axoniq.axonserver.accesscontrol.admin-token) with access control ENABLED ");
        }
        if (messagingPlatformConfiguration.getAccesscontrol().getToken() == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "Missing required token (axoniq.axonserver.accesscontrol.token) with access control ENABLED ");
        }
    }

    @Override
    public boolean allowed(String fullMethodName, String context, String token) {
        return isTokenFromConfigFile(token);
    }

    @Override
    public boolean allowed(String fullMethodName, String context, Authentication authentication) {
        Set<String> requiredRoles = rolesForOperation(fullMethodName);
        if (requiredRoles.isEmpty()) {
            return true;
        }

        if (authentication instanceof UsernamePasswordAuthenticationToken) {
            User user = userController.findUser(authentication.getName());
            if (user != null) {
                return user.getRoles().stream().anyMatch(r -> requiredRoles.contains(r.getRole()));
            }
        }

        return authentication.getAuthorities()
                             .stream()
                             .anyMatch(a -> requiredRoles.contains(a.getAuthority()));
    }

    private Set<String> rolesForOperation(String operation) {
        if (operation.contains("/v1/users")) {
            return Collections.singleton("ADMIN");
        }
        return Collections.emptySet();
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
