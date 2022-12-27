/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DefaultLocalTransformationCancelExecutor implements LocalTransformationCancelExecutor {

    @FunctionalInterface
    public interface DeleteActionSupplier {

        Mono<Void> delete(String context, String transformationId);
    }

    private static final Logger logger = LoggerFactory.getLogger(DefaultLocalEventStoreCompactionExecutor.class);
    private final DeleteActionSupplier deleteActionSupplier;

    private final Set<String> cancellingTransformations = new CopyOnWriteArraySet<>();

    public DefaultLocalTransformationCancelExecutor(DeleteActionSupplier deleteActionSupplier) {
        this.deleteActionSupplier = deleteActionSupplier;
    }

    @Override
    public Mono<Void> cancel(Transformation transformation) {
        return Mono.fromSupplier(() -> cancellingTransformations.add(transformation.id()))
                   .filter(inactive -> inactive) // this filter is needed to avoid invoking cancel more than once
                   .switchIfEmpty(Mono.error(new RuntimeException("The cancel operation is already in progress")))
                   .then(deleteActionSupplier.delete(transformation.context(), transformation.id()))
                   .doOnSuccess(v -> logger.info("Transformation {} cancelled successfully from local store.",
                                                 transformation))
                   .doOnError(t -> logger.info("Failed to cancel from the local store the transformation {}",
                                               transformation))
                   .doFinally(s -> cancellingTransformations.remove(transformation.id()));
    }
}
