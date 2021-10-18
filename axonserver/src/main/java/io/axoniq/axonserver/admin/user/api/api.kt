package io.axoniq.axonserver.admin.user.api

import io.axoniq.axonserver.access.jpa.User


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
    fun createOrUpdateUser(userName: String, password: String, roles: Set<UserRole>)

    /**
     * Retrieves all users defines in AxonServer.
     * @return a list of users
     */
    fun users(): List<User> = emptyList()

    /**
     * Delete an AxonServer user.
     * @param name the user's username
     */
    fun deleteUser(name: String)
}

interface UserRole {
    fun role():String
    fun context(): String
}