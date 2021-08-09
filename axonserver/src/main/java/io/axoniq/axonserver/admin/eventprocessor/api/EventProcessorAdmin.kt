package io.axoniq.axonserver.admin.eventprocessor.api

import io.axoniq.axonserver.api.Authentication
import reactor.core.publisher.Flux

/**
 *
 * @author Sara Pellegrini
 * @since 4.6
 */
interface EventProcessorAdminService {
    fun pause(identifier: EventProcessorId, authentication: Authentication)
    fun eventProcessorsForContext(context: String, authentication: Authentication): Flux<EventProcessorState>
}

data class EventProcessorId(val name: String, val context: String, val tokenStoreIdentifier: String);
data class EventProcessorState(val identifier: EventProcessorId, val segments: List<ClaimedSegmentState>)
data class ClaimedSegmentState(val clientId: String, val segmentId: Int, val partOf: Int)