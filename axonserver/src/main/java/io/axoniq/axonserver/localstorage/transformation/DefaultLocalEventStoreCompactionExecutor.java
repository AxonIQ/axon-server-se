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

public class DefaultLocalEventStoreCompactionExecutor implements LocalEventStoreCompactionExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLocalEventStoreCompactionExecutor.class);

    private final LocalEventStoreTransformer localEventStoreTransformer;
    private final Set<String> compactingContexts = new CopyOnWriteArraySet<>();

    public DefaultLocalEventStoreCompactionExecutor(LocalEventStoreTransformer localEventStoreTransformer) {
        this.localEventStoreTransformer = localEventStoreTransformer;
    }

    @Override
    public Mono<Void> compact(Compaction compaction) {
        return Mono.fromSupplier(() -> compactingContexts.add(compaction.context()))
                   .filter(inactive -> inactive) // this filter is needed to avoid invoking rollback more than once
                   .switchIfEmpty(Mono.error(new RuntimeException("The compaction operation is already in progress")))
                   .then(localEventStoreTransformer.compact(compaction.context()))
                   .doOnSuccess(v -> logger.info("Context {} successfully compacted in local store.",
                                                 compaction.context()))
                   .doOnError(t -> logger.info("Failed to compact the local store for context {}",
                                               compaction.context()))
                   .doFinally(s -> compactingContexts.remove(compaction.context()));
    }
}
