/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

/**
 * Wrapper around a StreamObserver for receiving GRPC messages. Catches exceptions on handling of the message and logs them.
 * This avoids closing the GRPC connection when an exception occurs on processing of the message.
 * @author Marc Gathier
 */
public abstract class ReceivingStreamObserver<T> implements StreamObserver<T> {
    private final Logger logger;

    protected ReceivingStreamObserver(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void onNext(T message) {
        try {
            consume(message);
        } catch( RuntimeException cause) {
            logger.warn("{}: Processing of message failed", sender(), cause);
        }
    }


    protected abstract void consume(T message);

    protected abstract String sender();

}
