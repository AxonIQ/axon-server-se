/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.eventstore.transformation.apply.TransformationApplyExecutor;
import reactor.core.publisher.Mono;

public class StandardTransformationApplyExecutor implements TransformationApplyExecutor {

    private final LocalTransformationApplyExecutor localTransformationApplyExecutor;

    public StandardTransformationApplyExecutor(LocalTransformationApplyExecutor localTransformationApplyExecutor) {
        this.localTransformationApplyExecutor = localTransformationApplyExecutor;
    }

    @Override
    public Mono<Void> apply(Transformation transformation) {
        return localTransformationApplyExecutor.apply(map(transformation))
                                               .then(transformation.markAsApplied());
        // TODO: 6/2/22 delete transformation actions
    }

    private LocalTransformationApplyExecutor.Transformation map(Transformation transformation) {
        return new LocalTransformationApplyExecutor.Transformation() {
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

            @Override
            public long lastSequence() {
                return transformation.lastSequence();
            }
        };
    }
}
