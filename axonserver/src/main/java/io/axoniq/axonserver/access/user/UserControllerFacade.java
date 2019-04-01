/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.user;


import io.axoniq.axonserver.access.jpa.User;

import java.util.List;


/**
 * Defined the interface to retrieve and update AxonServer users.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public interface UserControllerFacade {

    /**
     * Creates or updates a user.
     * @param username the username
     * @param password the password
     * @param roles an array of roles for the user
     */
    void updateUser(String username, String password, String[] roles);

    /**
     * Retrieves all users defines in AxonServer.
     * @return a list of users
     */
    List<User> getUsers();

    /**
     * Delete an AxonServer user.
     * @param name the user's username
     */
    void deleteUser(String name);
}
