/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.BaseSubscriber;

import javax.annotation.Nonnull;

/**
 * @author Marc Gathier
 * @since
 */
public class ConfirmationSubscriber extends BaseSubscriber<Void> {

    private final StreamObserver<Confirmation> responseObserver;

    public ConfirmationSubscriber(StreamObserver<Confirmation> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    protected void hookOnComplete() {
        responseObserver.onNext(confirmation());
        responseObserver.onCompleted();
    }

    @Override
    protected void hookOnError(@Nonnull Throwable throwable) {
        responseObserver.onError(GrpcExceptionBuilder.build(
                throwable));
    }

    private Confirmation confirmation() {
        return Confirmation.newBuilder().setSuccess(true).build();
    }
}
