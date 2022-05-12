/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;

import javax.annotation.Nonnull;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class VoidStreamObserverSubscriber extends BaseSubscriber<Void> {

    private static final Logger logger = LoggerFactory.getLogger(VoidStreamObserverSubscriber.class);

    private final String description;
    private final StreamObserver<Empty> responseObserver;

    public VoidStreamObserverSubscriber(StreamObserver<Empty> responseObserver) {
        this(responseObserver, "");
    }

    public VoidStreamObserverSubscriber(StreamObserver<Empty> responseObserver, String description) {
        this.responseObserver = responseObserver;
        this.description = description;
    }

    @Override
    protected void hookOnComplete() {
        logger.trace(description + " completed.");
        responseObserver.onCompleted();
    }

    @Override
    protected void hookOnError(@Nonnull Throwable throwable) {
        logger.error(description + " errored.", throwable);
        responseObserver.onError(GrpcExceptionBuilder.build(throwable));
    }
}
