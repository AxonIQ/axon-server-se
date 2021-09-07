package io.axoniq.axonserver.admin.eventprocessor.api

import io.axoniq.axonserver.api.Authentication
import reactor.core.publisher.Flux

/**
 *
 * @author Stefan Dragisic
 * @author Sara Pellegrini
 * @since 4.6
 */
interface EventProcessorAdminService {
    fun pause(identifier: EventProcessorId, authentication: Authentication)
    fun eventProcessors(authentication: Authentication): Flux<EventProcessorState>
}

interface EventProcessorId {
    fun name(): String;
    fun tokenStoreIdentifier(): String;
}

interface EventProcessorState {
    fun identifier(): EventProcessorId;
    fun segments(): List<ClaimedSegmentState>;
}

interface ClaimedSegmentState {
    fun clientId(): String;
    fun segmentId(): Int;
    fun onePartOf(): Int;
}