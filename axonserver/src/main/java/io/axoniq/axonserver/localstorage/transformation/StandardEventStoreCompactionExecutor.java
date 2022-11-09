/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.eventstore.transformation.compact.EventStoreCompactionExecutor;
import reactor.core.publisher.Mono;

public class StandardEventStoreCompactionExecutor implements EventStoreCompactionExecutor {

    private final LocalEventStoreCompactionExecutor compactionExecutor;

    public StandardEventStoreCompactionExecutor(
            LocalEventStoreCompactionExecutor compactionExecutor) {
        this.compactionExecutor = compactionExecutor;
    }

    @Override
    public Mono<Void> compact(Compaction transformation) {
        return compactionExecutor.compact(map(transformation))
                                 .then(transformation.markCompacted());
    }

    private LocalEventStoreCompactionExecutor.Compaction map(Compaction transformation) {
        return transformation::context;
    }
}
