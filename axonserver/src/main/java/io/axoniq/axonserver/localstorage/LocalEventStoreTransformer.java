/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.file.TransformationProgress;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 * @since
 */
public interface LocalEventStoreTransformer {

    void deleteOldVersions(String context, int version);

    void rollbackSegments(String context, int version);

    CompletableFuture<Void> transformEvents(String context, long firstToken, long lastToken,
                                            boolean keepOldVersions,
                                            int version,
                                            BiFunction<Event,Long,EventTransformationResult> transformationFunction,
                                            Consumer<TransformationProgress> transformationProgressConsumer);
}
