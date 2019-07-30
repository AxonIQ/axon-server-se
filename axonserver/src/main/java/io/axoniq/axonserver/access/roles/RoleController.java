/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.roles;

/**
 * Access to user and application roles.
 *
 * @author Sara Pellegrini
 */

import io.axoniq.axonserver.access.jpa.Role;
import org.springframework.stereotype.Controller;

import java.util.List;

@Controller
public class RoleController {

    private final RoleRepository roleRepository;

    public RoleController(RoleRepository roleRepository) {
        this.roleRepository = roleRepository;
    }


    public List<Role> listRoles() {
        return roleRepository.findAll();
    }


}
