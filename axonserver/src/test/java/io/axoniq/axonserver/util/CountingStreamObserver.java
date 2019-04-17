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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marc Gathier
 */
public class CountingStreamObserver<T> implements StreamObserver<T> {
    public int count;
    public Throwable error;
    public boolean completed;
    public List<T> responseList = new ArrayList<>();

    @Override
    public void onNext(T queryResponse) {
        responseList.add(queryResponse);
        count++;
    }

    @Override
    public void onError(Throwable throwable) {
        error = throwable;
    }

    @Override
    public void onCompleted() {
        completed = true;
    }
}
