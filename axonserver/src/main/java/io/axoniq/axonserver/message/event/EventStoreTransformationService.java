/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface EventStoreTransformationService {

    Mono<String> startTransformation(String context);

    Mono<Void> transformEvents(String context, Flux<TransformEventsRequest> flux);

    Mono<Void> cancelTransformation(String context, String id);

    Mono<Void> applyTransformation(String context, String id);
}
