/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;


import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.access.roles.RoleController;
import io.axoniq.axonserver.access.jpa.Role;
import io.axoniq.axonserver.access.user.UserControllerFacade;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.Valid;

import static io.axoniq.axonserver.util.StringUtils.sanitize;

/**
 * Rest services to manage users.
 * @author Marc Gathier
 * @since 4.0
 */
@RestController("UserRestController")
@CrossOrigin
@RequestMapping("/v1")
public class UserRestController {

    private static final Logger logger = LoggerFactory.getLogger(UserRestController.class);
    private static final Logger auditLog = AuditLog.getLogger();

    private final UserControllerFacade userController;
    private final RoleController roleController;

    public UserRestController(UserControllerFacade userController,
                              RoleController roleController) {
        this.userController = userController;
        this.roleController = roleController;
    }

    @PostMapping("users")
    public void createUser(@RequestBody @Valid UserJson userJson, Principal principal) {
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to create user \"{}\" with roles {}.",
                          AuditLog.username(principal),
                          sanitize(userJson.getUserName()),
                          Arrays.stream(userJson.getRoles())
                                .map(StringUtils::sanitize)
                                .collect(Collectors.toSet()));
        }

        if (userJson.userName != null && principal != null && userJson.userName.equals(principal.getName())) {
            throw new MessagingPlatformException(ErrorCode.AUTHENTICATION_INVALID_TOKEN,
                                                 "Not allowed to change your own credentials");
        }

        Set<String> validRoles = roleController.listRoles().stream().map(Role::getRole).collect(Collectors.toSet());
        Set<UserRole> roles = new HashSet<>();
        if (userJson.roles != null) {
            roles = Arrays.stream(userJson.roles).map(UserRole::parse).collect(Collectors.toSet());
            for (UserRole role : roles) {
                if (!validRoles.contains(role.getRole())) {
                    auditLog.error("[{}] Request to create user \"{}\" with roles {} FAILED: Unknown role \"{}\".",
                                   AuditLog.username(principal),
                                   sanitize(userJson.getUserName()),
                                   roles,
                                   role);
                    throw new MessagingPlatformException(ErrorCode.UNKNOWN_ROLE,
                                                         role + ": Role unknown");
                }
            }
        }
        auditLog.info("[{}] Create user \"{}\" with translated roles {}.", AuditLog.username(principal), userJson.getUserName(), roles);
        userController.updateUser(userJson.userName, userJson.password, roles);
    }

    @GetMapping("public/users")
    public List<UserJson> listUsers(Principal principal) {
        auditLog.info("[{}] Request to list users and their roles.", AuditLog.username(principal));
        try {
            return userController.getUsers().stream().map(UserJson::new).sorted(Comparator.comparing(UserJson::getUserName)).collect(Collectors.toList());
        } catch (Exception exception) {
            logger.info("[{}] List users failed - {}", AuditLog.username(principal), exception.getMessage(), exception);
            throw new MessagingPlatformException(ErrorCode.OTHER, exception.getMessage());
        }
    }

    @DeleteMapping(path = "users/{name}")
    public void dropUser(@PathVariable("name") String name, Principal principal) {
        try {
            auditLog.info("[{}] Request to delete user \"{}\".", AuditLog.username(principal), name);
            userController.deleteUser(name);
        } catch (Exception exception) {
            auditLog.error("[{}] Delete user {} failed - {}", AuditLog.username(principal), name, exception.getMessage());
            throw new MessagingPlatformException(ErrorCode.OTHER, exception.getMessage());
        }
    }

    public static class UserJson {

        private String userName;
        private String password;
        private String[] roles;

        public UserJson() {
        }

        public UserJson(User u) {
            userName = u.getUserName();
            if (u.getRoles() != null) {
                roles = u.getRoles().stream()
                         .map(UserRole::toString)
                         .toArray(String[]::new);
            }
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String[] getRoles() {
            return roles;
        }

        public void setRoles(String[] roles) {
            this.roles = roles;
        }
    }
}
