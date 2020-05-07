/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around a GRPC StreamObserver that ensures thread safety for sending messages, as GRPC does not provide this by default.
 * @author Marc Gathier
 */
public class SendingStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> delegate;
    private static final Logger logger = LoggerFactory.getLogger(SendingStreamObserver.class);

    public SendingStreamObserver(StreamObserver<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onNext(T t) {
        try {
            synchronized (delegate) {
                delegate.onNext(t);
            }
        } catch( RuntimeException | OutOfMemoryError e) {
            StreamObserverUtils.error(delegate, e);
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        try {
            delegate.onError(GrpcExceptionBuilder.build(throwable));
        } catch( RuntimeException t) {
            logger.debug("Failed send error on connection", t);
        }
    }

    @Override
    public void onCompleted() {
        try {
            delegate.onCompleted();
        } catch( RuntimeException t) {
            logger.debug("Failed to complete connection", t);
        }
    }
}
