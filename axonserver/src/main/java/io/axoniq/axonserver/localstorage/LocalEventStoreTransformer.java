/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.localstorage.file.TransformationProgress;
import reactor.core.publisher.Flux;

/**
 * Operations to transform events in an event store.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface LocalEventStoreTransformer {

    /**
     * Transforms events in the event store. Returns a {@link Flux} of progress objects where an object is published
     * when the transformation function reaches a certain milestone. The flux is completed when the transformation is
     * completed.
     * <p>
     * The {@code keepOldVersions} flag may be overridden at the event store level, if the keep old version flag is set
     * globally or on the context.
     *
     * @param context                the name of the context for the event store
     * @param firstToken             the token of the first event to transform
     * @param lastToken              the token of the last event to transform
     * @param keepOldVersions        indicates if the requester wants to keep the old versions of the events
     * @param version                the new version number for the event store
     * @param transformationFunction function to apply on an event to transform it
     * @return a flux of progress information objects
     */
    Flux<TransformationProgress> transformEvents(String context, long firstToken, long lastToken,
                                                 boolean keepOldVersions,
                                                 int version,
                                                 EventTransformationFunction transformationFunction);

    /**
     * Deletes all events in the event store related to the given context.
     *
     * @param context the name of the context
     * @param version the version of the event store to delete
     */
    void deleteOldVersions(String context, int version);

    /**
     * Verifies if an event store can rollback changes made in a specific version. Rollback is possible if there are no
     * newer versions of events in the range between the first token and the last token, and there is a previous version
     * of the events.
     *
     * @param context         the name of the context
     * @param version         the version of the event store to rollback
     * @param firstEventToken the first token that was transformed in this version
     * @param lastEventToken  the last token that was transformed in this version
     * @return true if rollback is possible
     */
    boolean canRollbackTransformation(String context, int version, long firstEventToken, long lastEventToken);

    /**
     * Removes the events with given version, making the previous version of the events the active version.
     *
     * @param context the name of the context
     * @param version the version of the event store to rollback
     */
    void rollbackSegments(String context, int version);
}
