/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.roles;

import io.axoniq.axonserver.access.jpa.FunctionRole;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;

/**
 * Repository for FunctionRole mappings.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public interface FunctionRoleRepository extends JpaRepository<FunctionRole, Long> {

    Collection<FunctionRole> findByFunction(String function);
}
