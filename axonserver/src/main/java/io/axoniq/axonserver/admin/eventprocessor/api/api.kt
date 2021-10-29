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
     * The {@link Mono} completes when the request has been propagated to the clients.
     * It doesn't guarantee that the request has been processed by all clients.
     *
     * @param identifier     the event processor identifier
     * @param authentication info about the authenticated user
     */
    fun pause(identifier: EventProcessorId, authentication: Authentication): Mono<Void>
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
