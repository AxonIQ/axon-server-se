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

interface SegmentTransformer {

    Mono<Void> initialize();

    Mono<Void> transformEvent(EventWithToken transformedEvent);

    Mono<Void> completeSegment();

    Mono<Void> rollback(Throwable e);

    Mono<Void> cancel();

    long segment();
}
