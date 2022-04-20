/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.user.api

import io.axoniq.axonserver.access.jpa.User
import io.axoniq.axonserver.api.Authentication


/**
 * Defined the interface to retrieve and update AxonServer users.
 *
 * @author Marc Gathier
 * @author Stefan Dragisic
 * @since 4.6.0
 */
interface UserAdminService {
    /**
     * Creates or updates a user.
     * @param userName the username
     * @param password the password
     * @param roles an array of roles for the user
     */
    fun createOrUpdateUser(userName: String, password: String, roles: Set<UserRole>, authentication: Authentication)

    /**
     * Retrieves all users defines in AxonServer.
     * @return a list of users
     */
    fun users(authentication: Authentication): List<User> = emptyList()

    /**
     * Delete an AxonServer user.
     * @param name the user's username
     */
    fun deleteUser(name: String, authentication: Authentication)
}

interface UserRole {
    fun role():String
    fun context(): String
}