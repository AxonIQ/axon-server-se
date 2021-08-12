/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.requestprocessor.eventstore;

import io.axoniq.axonserver.grpc.event.Event;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class DefaultEventStoreTransformationService implements EventStoreTransformationService {

    @Override
    public Mono<String> startTransformation(String context) {
        return Mono.error(new RuntimeException("Not implemented yet"));
    }

    @Override
    public Mono<Void> deleteEvent(String context, String transformationId, long token) {
        return Mono.error(new RuntimeException("Not implemented yet"));
    }

    @Override
    public Mono<Void> replaceEvent(String context, String transformationId, long token, Event event) {
        return Mono.error(new RuntimeException("Not implemented yet"));
    }

    @Override
    public Mono<Void> cancelTransformation(String context, String id) {
        return Mono.error(new RuntimeException("Not implemented yet"));
    }

    @Override
    public Mono<Void> applyTransformation(String context, String id, long lastEventToken, long lastSnapshotToken) {
        return Mono.error(new RuntimeException("Not implemented yet"));
    }
}
