package io.axoniq.axonserver.admin.eventprocessor.api

import io.axoniq.axonserver.api.Authentication


/**
 * Component to perform operations related to event processors.
 *
 * @author Stefan Dragisic
 * @author Sara Pellegrini
 * @since 4.6
 */
interface EventProcessorAdminService {

    /**
     * Handles a request to pause a certain event processor.
     * Returns when the request has been propagated to the clients.
     * It doesn't guarantee that the request has been processed by all clients.
     */
    fun pause(identifier: EventProcessorId, authentication: Authentication)

    /**
     * Handles a request to start a certain event processor.
     * Returns when the request has been propagated to the clients.
     * It doesn't guarantee that the request has been processed by all clients.
     */
    fun start(identifier: EventProcessorId, authentication: Authentication)

    /**
     * Handles a request to split the biggest segment of a certain event processor.
     * Returns when the request has been propagated to the interested client.
     * It doesn't guarantee that the request has been processed by the client.
     */
    fun split(identifier: EventProcessorId, authentication: Authentication)

    /**
     * Handles a request to merge the two smallest segments of a certain event processor.
     * Returns when the request has been propagated to the interested clients.
     * It doesn't guarantee that the request has been processed by the clients.
     */
    fun merge(identifier: EventProcessorId, authentication: Authentication)

    /**
     * Handles a request to move a segment from the client that claimed it to the target client.
     * Returns when the request has been propagated to the interested clients.
     * It doesn't guarantee that the request has been processed the clients.
     */
    fun move(identifier: EventProcessorId, segment: Int, target: String, authentication: Authentication)
}
/**
 * Identifier for event processor.
 */
interface EventProcessorId {
    /**
     * Returns event processor name
     */
    fun name(): String;

    /**
     * Returns token store identifier
     */
    fun tokenStoreIdentifier(): String;
}
