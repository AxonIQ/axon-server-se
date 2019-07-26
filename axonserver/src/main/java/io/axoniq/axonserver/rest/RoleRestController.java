/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.access.role.RoleController;
import io.axoniq.axonserver.access.roles.Role;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

import static java.util.stream.Collectors.toList;

/**
 * Rest service to retrieve user roles and application roles.
 * @author Sara Pellegrini
 * @since 4.0
 *
 */
@RestController
@RequestMapping("v1/roles")
public class RoleRestController {

    private final RoleController roleController;

    public RoleRestController(RoleController roleController) {
        this.roleController = roleController;
    }

    @GetMapping("user")
    Collection<String> listUserRoles(){
        return roleController
                .listRoles()
                .stream().map(Role::getRole).sorted().collect(toList());
    }

    @GetMapping("application")
    Collection<String> listApplicationRoles(){
        return roleController
                .listRoles()
                .stream().map(Role::getRole).sorted().collect(toList());
    }



}
