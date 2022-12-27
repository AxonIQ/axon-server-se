/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.eventstore.transformation.cancel.TransformationCancelExecutor;
import reactor.core.publisher.Mono;

public class StandardTransformationCancelExecutor implements TransformationCancelExecutor {

    private final LocalTransformationCancelExecutor localTransformationCancelExecutor;

    public StandardTransformationCancelExecutor(LocalTransformationCancelExecutor localTransformationCancelExecutor) {
        this.localTransformationCancelExecutor = localTransformationCancelExecutor;
    }

    @Override
    public Mono<Void> cancel(Transformation transformation) {
        return localTransformationCancelExecutor.cancel(map(transformation));
    }

    private LocalTransformationCancelExecutor.Transformation map(Transformation transformation) {
        return new LocalTransformationCancelExecutor.Transformation() {
            @Override
            public String id() {
                return transformation.id();
            }

            @Override
            public String context() {
                return transformation.context();
            }

            @Override
            public int version() {
                return transformation.version();
            }
        };
    }
}
