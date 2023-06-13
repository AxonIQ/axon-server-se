/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import reactor.core.publisher.Mono;
/**
 * Transforms a group of events that must be treated a single unit, called Segment.
 */
interface SegmentTransformer {

    /**
     * Initializes the segment transformation.
     * @return a Mono which completes successfully when the segment transformation is initialized.
     */
    Mono<Void> initialize();

    /**
     * Store the transformed event
     * @param transformedEvent the replacement event
     * @return a Mono which completes successfully when the transformed event is stored
     */
    Mono<Void> transformEvent(EventWithToken transformedEvent);

    /**
     * Completes the segment transformation.
     *
     * @return a Mono, which completes successfully with the number of events transformed inside the segment, or
     * completes exceptionally any time a problem occurs during completion.
     */
    Mono<Long> completeSegment();

    /**
     * Rollbacks the segment transformation.
     * @return a Mono which completes successfully when the segment transformation is rolled back.
     */
    Mono<Void> rollback(Throwable e);

    Mono<Void> cancel();

    long segment();
}
