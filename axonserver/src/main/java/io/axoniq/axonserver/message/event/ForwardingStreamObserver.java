/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.stream.CallStreamObserverDelegator;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

/**
 * Wraps a {@link StreamObserver} to translate errors to standard exceptions ({@link io.grpc.StatusRuntimeException} with error code in meta data).
 * @author Marc Gathier
 * @since 4.0
 */
public class ForwardingStreamObserver<T> extends CallStreamObserverDelegator<T> {

    private final Logger logger;
    private final String request;

    public ForwardingStreamObserver(
            Logger logger, String request, CallStreamObserver<T> responseObserver) {
        super(responseObserver);
        this.logger = logger;
        this.request = request;
    }

    @Override
    public void onError(Throwable cause) {
        logger.debug(EventDispatcher.ERROR_ON_CONNECTION_FROM_EVENT_STORE, request, cause.getMessage());
        delegate().onError(GrpcExceptionBuilder.build(cause));
    }
}
