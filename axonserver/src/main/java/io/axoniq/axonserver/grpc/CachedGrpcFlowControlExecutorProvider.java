/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A GrpcFlowControlExecutorProvider that use a cached thread pool implementation.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 * @author Stefan Dragisic
 * @since 4.5
 */
@Component
public class CachedGrpcFlowControlExecutorProvider implements GrpcFlowControlExecutorProvider {

    private final ExecutorService executor;

    public CachedGrpcFlowControlExecutorProvider(MeterRegistry meterRegistry) {
        this.executor = ExecutorServiceMetrics.monitor(meterRegistry,
                                                       Executors.newCachedThreadPool(),
                                                       "CachedGrpcFlowControlExecutor",
                                                       Collections.emptySet());
    }

    public Executor provide() {
        return executor;
    }

    @PreDestroy
    private void shutdown() {
        executor.shutdown();
    }
}
