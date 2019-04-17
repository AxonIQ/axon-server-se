/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class FailingStreamObserver<T> implements StreamObserver<T> {

    @Override
    public void onNext(T request) {
        throw new RuntimeException("Failing onNext");

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onCompleted() {

    }
}
