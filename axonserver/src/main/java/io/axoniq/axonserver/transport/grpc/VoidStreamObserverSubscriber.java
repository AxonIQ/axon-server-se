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
import reactor.core.publisher.BaseSubscriber;

import javax.annotation.Nonnull;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class VoidStreamObserverSubscriber extends BaseSubscriber<Void> {

    private final StreamObserver<Empty> responseObserver;

    public VoidStreamObserverSubscriber(StreamObserver<Empty> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    protected void hookOnComplete() {
        responseObserver.onCompleted();
    }

    @Override
    protected void hookOnError(@Nonnull Throwable throwable) {
        throwable.printStackTrace();
        responseObserver.onError(GrpcExceptionBuilder.build(throwable));
    }
}
