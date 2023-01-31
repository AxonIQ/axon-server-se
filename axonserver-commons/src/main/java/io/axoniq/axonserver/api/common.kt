package io.axoniq.axonserver.api

/**
 * Info about the authenticated user
 *
 * @author Stefan Dragisic
 * @author Sara Pellegrini
 * @since 4.6
 */
interface Authentication {
    /**
     * Returns the username of the authenticated user
     */
    fun username(): String

    /**
     * Returns {@code true} if the authentication contains the given {@code role} for the {@code context}.
     * @param role the role to check
     * @param context the context to check
     * @return true if the authentication contains the given role for the context.
     */
    fun hasRole(role: String, context: String): Boolean

    /**
     * Returns {@code true} if the Authentication is based on an application token
     */
    fun application(): Boolean

}