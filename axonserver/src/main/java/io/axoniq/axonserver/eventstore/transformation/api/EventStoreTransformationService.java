/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.api;

import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface EventStoreTransformationService {

    Mono<String> startTransformation(String context);

    Mono<Void> deleteEvent(String context, String transformationId, long token, long previousToken);

    Mono<Void> replaceEvent(String context, String transformationId, long token, Event event, long previousToken);

    Mono<Void> cancelTransformation(String context, String id);

    Mono<Void> applyTransformation(String context, String id, long lastEventToken, boolean keepOldVersions);

    Mono<Void> rollbackTransformation(String context, String id);

    Mono<Void> deleteOldVersions(String context, String id);
}
