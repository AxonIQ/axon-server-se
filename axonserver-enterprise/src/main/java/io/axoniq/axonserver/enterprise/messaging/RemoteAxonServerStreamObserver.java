package io.axoniq.axonserver.enterprise.messaging;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.grpc.stub.StreamObserver;

/**
 * Author: marc
 */
public class RemoteAxonServerStreamObserver<T> implements StreamObserver<T> {

    private final StreamObserver<T> delegate;

    public RemoteAxonServerStreamObserver(StreamObserver<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onNext(T t) {
        delegate.onNext(t);
    }

    @Override
    public void onError(Throwable throwable) {
        MessagingPlatformException messagingPlatformException = GrpcExceptionBuilder.parse(throwable);
        delegate.onError(messagingPlatformException);
    }

    @Override
    public void onCompleted() {
        delegate.onCompleted();
    }
}
