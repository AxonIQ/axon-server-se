package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.event.Event;
import io.grpc.stub.StreamObserver;

/**
 * Author: marc
 */
public class NoOpStreamObserver<T> implements StreamObserver<T> {

    @Override
    public void onNext(T t) {

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onCompleted() {

    }
}
