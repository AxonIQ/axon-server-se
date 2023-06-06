/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.api;

import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Service to perform transformations on an event store.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface EventStoreTransformationService {

    /**
     * Returns the transformations for the specified context and identifier.
     *
     * @param id             the identifier of the event transformation
     * @param context        the context
     * @param authentication authentication of the user/application requesting the service
     * @return the transformations for the specified context and identifier.
     */
    default Mono<Transformation> transformation(String id, String context, @Nonnull Authentication authentication) {
        return transformations(context, authentication).filter(transformation -> id.equals(transformation.id()))
                                                       .next();
    }

    /**
     * Initializes a new transformation.
     *
     * @param id             the identifier of the transformation
     * @param context        the name of the context
     * @param description    description of the goal of the transformation
     * @param authentication authentication of the user/application requesting the service
     * @return a mono that completes once the transformation has been started
     */
    Mono<Void> start(String id,
                     String context,
                     String description,
                     @Nonnull Authentication authentication);

    /**
     * Returns the transformations for the specified context.
     *
     * @param context        the name of the context
     * @param authentication authentication of the user/application requesting the service
     * @return the transformations for the specified context.
     */
    Flux<Transformation> transformations(String context, @Nonnull Authentication authentication);

    /**
     * Registers the intent to replace the content of an event when applying the transformation. The caller needs to
     * provide the previous token to ensure that the events to change in the transformation are specified in the correct
     * order.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param token            the token (global position) of the event to replace
     * @param event            the new content of the event
     * @param sequence         the sequence of the transformation request used to validate the requests chain, -1 if it
     *                         is the first one
     * @param authentication   authentication of the user/application requesting the service
     * @return a mono that is completed when the replace event action is registered
     */
    Mono<Void> replaceEvent(String context, String transformationId, long token, Event event, long sequence,
                            @Nonnull Authentication authentication);

    /**
     * Registers the intent to delete an event when applying the transformation. The caller needs to provide the
     * previous sequence to ensure that the transformation actions are received in the correct order.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param token            the token (global position) of the event to be deleted
     * @param sequence         the sequence of the transformation request used to validate the request chain, -1 if it
     *                         is the first one
     * @param authentication   authentication of the user/application requesting the service
     * @return a mono that is completed when the delete event action is registered
     */
    Mono<Void> deleteEvent(String context, String transformationId, long token, long sequence,
                           @Nonnull Authentication authentication);

    /**
     * Starts the compaction process. The process runs in the background.
     *
     * @param compactionId   the unique identifier of this compaction request
     * @param context        the name of the context
     * @param authentication authentication of the user/application requesting the service
     * @return a mono that is completed when the compaction process has been started
     */
    Mono<Void> startCompacting(String compactionId, String context,
                               @Nonnull Authentication authentication);

    /**
     * Cancels a transformation. Can only be done before calling the
     * {@link EventStoreTransformationService#startApplying(String, String, long, Authentication)} operation.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param authentication   authentication of the user/application requesting the service
     * @return a mono that is completed when the transformation is cancelled
     */
    Mono<Void> cancel(String context, String transformationId, @Nonnull Authentication authentication);

    /**
     * Starts the apply process. The process runs in the background.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param sequence         the sequence of the last transformation request used to validate the requests chain
     * @param authentication   authentication of the user/application requesting the service
     * @return a mono that is completed when applying the transformation is started
     */
    Mono<Void> startApplying(String context, String transformationId, long sequence,
                             @Nonnull Authentication authentication);

    /**
     * The transformation data.
     */
    interface Transformation {

        /**
         * Returns the identifier of the event transformation.
         *
         * @return the identifier of the event transformation.
         */
        String id();

        /**
         * Returns the context of the event transformation.
         *
         * @return the context of the event transformation.
         */
        String context();

        /**
         * Returns the description of the event transformation.
         *
         * @return the description of the event transformation.
         */
        String description();

        /**
         * Returns the versions of the event transformation.
         * It is a sequential number that determine the order the transformations have been created.
         *
         * @return the version of the event transformation.
         */
        int version();

        /**
         * Returns an {@link Optional} of the last sequence for the event transformation.
         * The last sequence is a sequential number that is increased any time a new operation is registered in the
         * transformation. This number is important since it is used to guarantee the correct order of the operations
         * registered and avoid gaps caused by any problem like network communication issues.
         *
         * @return an {@link Optional} of the last sequence for the event transformation.
         */
        Optional<Long> lastSequence();

        /**
         * Returns the status of the event transformation.
         *
         * @return the status of the event transformation.
         */
        Status status();


        /**
         * Returns the {@link Optional} requester username. The optional is empty if no apply request has been received.
         *
         * @return the {@link Optional} requester username.
         */
        Optional<String> applyRequester();

        /**
         * Returns the {@link Optional} {@link Instant} when the transformation has been applied.
         * The optional is empty if the transformation is not applied yet.
         *
         * @return the {@link Optional} {@link Instant} when the transformation has been applied.
         */
        Optional<Instant> appliedAt();


        /**
         * The status of the event transformation.
         */
        enum Status {
            /**
             * When the event transformation can accept the registration of the transforming operations.
             */
            ACTIVE,

            /**
             * When the event transformation has been cancelled. It is a final state.
             */
            CANCELLED,
            /**
             * When the transforming operation that has been registered in the event transformation are going to be
             * applied to the event store. This state is valid during all the time necessary to apply all the
             * operations.
             * During this period, the event store could be in a mixed state, partially transformed.
             */
            APPLYING,
            /**
             * When the event transformation has been fully applied. It is a final state.
             */
            APPLIED
        }
    }
}
