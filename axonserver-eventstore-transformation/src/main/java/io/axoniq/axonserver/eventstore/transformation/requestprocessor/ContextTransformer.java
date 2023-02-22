package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;

/**
 * Performs all operations related to the event transformation for a single context.
 */
public interface ContextTransformer {

    /**
     * Initializes a new transformation.
     *
     * @param id          the identifier of the transformation
     * @param description the description of the transformation
     * @return a mono that completes once the transformation has been started
     */
    Mono<Void> start(String id, String description);

    /**
     * Registers the intent to delete an event when applying the transformation. The caller needs to provide the
     * previous sequence to ensure that the transformation actions are received in the correct order.
     *
     * @param transformationId the identification of the transformation
     * @param token            the token (global position) of the event to be deleted
     * @param sequence         the sequence of the transformation request used to validate the request chain, -1 if it
     *                         is the first one
     * @return a mono that completes once the delete event operation has been registered
     */
    Mono<Void> deleteEvent(String transformationId, long token, long sequence);

    /**
     * Registers the intent to replace the content of an event when applying the transformation. The caller needs to
     * provide the previous token to ensure that the events to change in the transformation are specified in the correct
     * order.
     *
     * @param transformationId the identification of the transformation
     * @param token            the token (global position) of the event to replace
     * @param event            the new content of the event
     * @param sequence         the sequence of the transformation request used to validate the requests chain, -1 if it
     *                         is the first one
     * @return a mono that is completed when the replace event action is registered
     */
    Mono<Void> replaceEvent(String transformationId, long token, Event event, long sequence);

    /**
     * Cancels a transformation. Can only be done before calling the
     * {@link EventStoreTransformationService#startApplying(String, String, long, Authentication)} operation.
     *
     * @param transformationId the identification of the transformation
     * @return a mono that is completed when the transformation is cancelled
     */
    Mono<Void> cancel(String transformationId);

    /**
     * Mark the transformation as "to be applied".
     *
     * @param transformationId the identification of the transformation
     * @param sequence         the sequence of the last transformation request used to validate the requests chain
     * @param applier          the application/user that requested to start applying the transformation
     * @return a mono that is completed when applying the transformation has been marked as "to be applied".
     */
    Mono<Void> startApplying(String transformationId, long sequence, String applier);

    /**
     * Mark the transformation as "applied".
     *
     * @param transformationId the identification of the transformation
     * @return a mono that is completed when applying the transformation has been marked as "applied".
     */
    Mono<Void> markApplied(String transformationId);

    /**
     * Mark the event store as "to be compacted".
     *
     * @param compactionId the unique identifier of this compaction request
     * @return a mono that is completed when the event store state has been marked as "to be compacted".
     */
    Mono<Void> startCompacting(String compactionId);

    /**
     * Mark the event store as "compacted".
     *
     * @param compactionId the unique identifier of this compaction request
     * @return a mono that is completed when the event store state has been marked as "compacted".
     */
    Mono<Void> markCompacted(String compactionId);

    Mono<Void> clean();
}
