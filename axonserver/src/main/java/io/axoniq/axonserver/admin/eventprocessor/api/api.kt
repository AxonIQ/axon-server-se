package io.axoniq.axonserver.admin.eventprocessor.api

import io.axoniq.axonserver.api.Authentication
import reactor.core.publisher.Mono


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
     * The returned {@link Mono} completes when the request has been propagated to the clients.
     * It doesn't guarantee that the request has been processed by all clients.
     *
     * @param identifier     the event processor identifier
     * @param authentication info about the authenticated user
     */
    fun pause(identifier: EventProcessorId, authentication: Authentication): Mono<Void>

    /**
     * Handles a request to start a certain event processor.
     * The returned {@link Mono} completes when the request has been propagated to the clients.
     * It doesn't guarantee that the request has been processed by all clients.
     *
     * @param identifier     the event processor identifier
     * @param authentication info about the authenticated user
     */
    fun start(identifier: EventProcessorId, authentication: Authentication): Mono<Void>

    /**
     * Handles a request to split the biggest segment of a certain event processor.
     * The returned {@link Mono} completes when the request has been propagated to the interested client.
     * It doesn't guarantee that the request has been processed by the client.
     *
     * @param identifier     the event processor identifier
     * @param authentication info about the authenticated user
     */
    fun split(identifier: EventProcessorId, authentication: Authentication): Mono<Void>

    /**
     * Handles a request to merge the two smallest segments of a certain event processor.
     * The returned {@link Mono} completes when the request has been propagated to the interested clients.
     * It doesn't guarantee that the request has been processed by the clients.
     *
     * @param identifier     the event processor identifier
     * @param authentication info about the authenticated user
     */
    fun merge(identifier: EventProcessorId, authentication: Authentication): Mono<Void>
}

/**
 * Identifier for event processor.
 */
interface EventProcessorId {
    /**
     * Returns event processor name
     */
    fun name(): String

    /**
     * Returns token store identifier
     */
    fun tokenStoreIdentifier(): String
}
