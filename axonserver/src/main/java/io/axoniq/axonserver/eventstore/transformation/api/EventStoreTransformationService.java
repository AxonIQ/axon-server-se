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
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * Service to perform transformations on an event store.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface EventStoreTransformationService {

    /**
     * Initializes a new transformation and returns a transformation id.
     * @param context the name of the context
     * @param description description of the goal of the transformation
     * @return a mono with a unique identifier for the transformation
     */
    Mono<String> startTransformation(String context, String description, @Nonnull Authentication authentication);

    /**
     * Registers the intent to delete an event when applying the transformation. The caller needs to provide the
     * previous token to ensure that the events to change in the transformation are specified in the correct order.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param token            the token (global position) of the event to delete
     * @param previousToken    the token of the token of the previous update in this transformation
     * @return a mono that is completed when the delete event action is registered
     */
    Mono<Void> deleteEvent(String context, String transformationId, long token, long previousToken,
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
     * @param previousToken    the token of the token of the previous update in this transformation
     * @return a mono that is completed when the delete event action is registered
     */
    Mono<Void> replaceEvent(String context, String transformationId, long token, Event event, long previousToken,
                            @Nonnull Authentication authentication);

    /**
     * Cancels a transformation. Can only be done before calling the applyTransformation operation.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @return a mono that is completed when the transformation is cancelled
     */
    Mono<Void> cancelTransformation(String context, String transformationId, @Nonnull Authentication authentication);

    /**
     * Starts the apply process. The process runs in the background.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param lastEventToken   the token of the last event included in this transformation
     * @param keepOldVersions  option to keep old versions of the event store segments, so that the transformation can
     *                         be rolled back
     * @param appliedBy        the user/application that started the apply process
     * @return a mono that is completed when applying the transformation is started
     */
    Mono<Void> applyTransformation(String context, String transformationId, long lastEventToken,
                                   boolean keepOldVersions,
                                   @Nonnull Authentication authentication);

    /**
     * Rolls back a previously applied transformation. Only the last transformation for a context can be rolled back.
     * @param context the name of the context
     * @param transformationId the identification of the transformation
     * @return a mono that is completed when the rollback is completed successfully
     */
    Mono<Void> rollbackTransformation(String context, String transformationId, @Nonnull Authentication authentication);

    /**
     * Deletes old versions of segments updated by a transformation (if the transformation was applied with the {@code keepOldVersions} option)
     * @param context the name of the context
     * @param transformationId the identification of the transformation
     * @return a mono that is completed when the operation is completed successfully
     */
    Mono<Void> deleteOldVersions(String context, String transformationId, @Nonnull Authentication authentication);
}
