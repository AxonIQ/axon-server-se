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

import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Service to perform transformations on an event store.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface EventStoreTransformationService {

    interface Transformation { // TODO: 1/27/23 add missing fields, check proto file

        String id();

        String context();

        String description();

        int version();

        Optional<Long> lastSequence();

        Status status();


        enum Status {
            ACTIVE,
            CANCELLED,
            APPLYING,
            APPLIED,
            FAILED
        }
    }

    default Mono<Transformation> transformation(String id, String context, @Nonnull Authentication authentication) {
        return transformations(context, authentication).filter(transformation -> id.equals(transformation.id()))
                                                       .next();
    }

    Flux<Transformation> transformations(String context, @Nonnull Authentication authentication);

    /**
     * Initializes a new transformation and returns a transformation id.
     *
     * @param id             the identifier of the transformation
     * @param context        the name of the context
     * @param description    description of the goal of the transformation
     * @param authentication authentication of the user/application requesting the service
     * @return a mono with a unique identifier for the transformation
     */
    Mono<Void> start(String id,
                     String context,
                     String description,
                     @Nonnull Authentication authentication);

    /**
     * Registers the intent to delete an event when applying the transformation. The caller needs to provide the
     * previous token to ensure that the events to change in the transformation are specified in the correct order.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param token            the token (global position) of the event to delete
     * @param sequence         the sequence of the transformation request used to validate the request chain, -1 if it
     *                         is the first one
     * @param authentication   authentication of the user/application requesting the service
     * @return a mono that is completed when the delete event action is registered
     */
    Mono<Void> deleteEvent(String context, String transformationId, long token, long sequence,
                           @Nonnull Authentication authentication);

    /**
     * Registers the intent to replace the content of an event when applying the transformation. The caller needs to
     * provide the previous token to ensure that the events to change in the transformation are specified in the correct
     * order.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param token            the token (global position) of the event to delete
     * @param event            the new content of the event
     * @param sequence         the sequence of the transformation request used to validate the requests chain, -1 if it
     *                         is the first one
     * @param authentication   authentication of the user/application requesting the service
     * @return a mono that is completed when the delete event action is registered
     */
    Mono<Void> replaceEvent(String context, String transformationId, long token, Event event, long sequence,
                            @Nonnull Authentication authentication);

    /**
     * Cancels a transformation. Can only be done before calling the applyTransformation operation.
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
     * Deletes old versions of segments updated by a transformation (if the transformation was applied with the
     * {@code keepOldVersions} option)
     *
     * @param compactionId   the unique identifier of this compaction request
     * @param context        the name of the context
     * @param authentication authentication of the user/application requesting the service
     * @return a mono that is completed when the operation is completed successfully
     */
    Mono<Void> compact(String compactionId, String context,
                       @Nonnull Authentication authentication); //TODO rename with startCompacting
}
