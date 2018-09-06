package io.axoniq.axonhub.util;

import io.grpc.stub.StreamObserver;

/**
 * Author: marc
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
