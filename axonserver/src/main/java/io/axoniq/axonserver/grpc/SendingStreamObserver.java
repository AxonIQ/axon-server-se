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
import io.grpc.stub.StreamObserver;
import io.netty.util.internal.OutOfDirectMemoryError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Wrapper around a GRPC StreamObserver that ensures thread safety for sending messages, as GRPC does not provide this by default.
 * @author Marc Gathier
 */
public class SendingStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> delegate;
    private static final Logger logger = LoggerFactory.getLogger(SendingStreamObserver.class);
    private final ReentrantLock guard = new ReentrantLock();

    public SendingStreamObserver(StreamObserver<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onNext(T t) {
        guard.lock();
        try {
            delegate.onNext(t);
        } catch( Throwable e) {
            logger.warn("Error while sending message: {}", e.getMessage(), e);
            try {
                // Cancel RPC
                delegate.onError(e);
            } catch (Throwable ex) {
                // Ignore further exception on cancelling the RPC
            }
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage());
        } finally {
            guard.unlock();
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
