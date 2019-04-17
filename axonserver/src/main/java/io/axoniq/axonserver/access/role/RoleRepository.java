/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.role;

import io.axoniq.axonserver.access.jpa.Role;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 *  Spring Data JpaRepostory to access {@link Role} entities.
 *
 * @author Sara Pellegrini
 */
public interface RoleRepository extends JpaRepository<Role, String> {

    /**
     * Finds defined roles based on the type.
     * @param type Application or User roles
     * @return list of roles
     */
    List<Role> findByTypesContains(Role.Type type);

}
