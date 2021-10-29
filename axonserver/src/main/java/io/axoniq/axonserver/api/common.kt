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
}