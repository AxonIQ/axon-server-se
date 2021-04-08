/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.security.access.user;

import io.axoniq.axonserver.refactoring.security.access.jpa.User;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JpaRepostory to access {@link User} entities.
 *
 * @author Marc Gathier
 */
public interface UserRepository extends JpaRepository<User, String> {

}
