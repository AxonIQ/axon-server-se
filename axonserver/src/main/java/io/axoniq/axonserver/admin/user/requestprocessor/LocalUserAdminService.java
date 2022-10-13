/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.user.requestprocessor;

import io.axoniq.axonserver.access.jpa.Role;
import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.access.roles.RoleController;
import io.axoniq.axonserver.admin.user.api.UserAdminService;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.applicationevents.UserEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.StringUtils;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static io.axoniq.axonserver.util.StringUtils.sanitize;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public class LocalUserAdminService implements UserAdminService {

    private static final Logger auditLog = AuditLog.getLogger();

    private final UserController userController;
    private final ApplicationEventPublisher eventPublisher;
    private final RoleController roleController;


    public LocalUserAdminService(UserController userController,
                                 ApplicationEventPublisher eventPublisher,
                                 RoleController roleController) {
        this.userController = userController;
        this.eventPublisher = eventPublisher;
        this.roleController = roleController;
    }

    @Override
    public void createOrUpdateUser(@Nonnull String userName, @Nonnull String password,
                                   @Nonnull Set<? extends io.axoniq.axonserver.admin.user.api.UserRole> roles,
                                   @Nonnull Authentication authentication) {
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to create user \"{}\" with roles {}.",
                          AuditLog.username(authentication.username()),
                          sanitize(userName),
                          roles.stream()
                               .map(role -> role.role() + "@" + role.context())
                               .map(StringUtils::sanitize)
                               .collect(Collectors.toSet()));
        }
        checkRoles(roles);
        Set<UserRole> userRoles = roles.stream().map(r -> new UserRole(r.context(), r.role()))
                                       .collect(Collectors.toSet());
        validateContexts(userRoles);
        User updatedUser = userController.updateUser(userName, password, userRoles);
        eventPublisher.publishEvent(new UserEvents.UserUpdated(updatedUser, false, authentication));
    }

    private void validateContexts(Set<UserRole> roles) {
        if (roles == null) {
            return;
        }
        if (roles.stream().anyMatch(userRole -> !validContext(userRole.getContext()))) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                 "Only specify context default for standard edition");
        }
    }

    private boolean validContext(String context) {
        return context == null || context.equals(Topology.DEFAULT_CONTEXT) || context.equals("*");
    }

    @Override
    public void deleteUser(@Nonnull String name, @Nonnull Authentication authentication) {
        auditLog.info("[{}] Request to delete user \"{}\".", AuditLog.username(authentication.username()), name);
        userController.deleteUser(name);
        eventPublisher.publishEvent(new UserEvents.UserDeleted(name, false, authentication));
    }

    @Nonnull
    @Override
    public List<User> users(@Nonnull Authentication authentication) {
        auditLog.info("[{}] Request to list users and their roles.", AuditLog.username(authentication.username()));
        return userController.getUsers();
    }

    private void checkRoles(Set<? extends io.axoniq.axonserver.admin.user.api.UserRole> rolesPerContext) {
        Set<String> validRoles = roleController.listRoles()
                                               .stream()
                                               .map(Role::getRole)
                                               .collect(Collectors.toSet());
        List<String> roles = rolesPerContext.stream()
                                            .map(io.axoniq.axonserver.admin.user.api.UserRole::role)
                                            .distinct()
                                            .collect(Collectors.toList());
        for (String role : roles) {
            if (!validRoles.contains(role)) {

                throw new MessagingPlatformException(ErrorCode.UNKNOWN_ROLE,
                                                     role + ": Role unknown");
            }
        }
    }
}
