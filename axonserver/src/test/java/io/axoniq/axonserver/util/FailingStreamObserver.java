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
