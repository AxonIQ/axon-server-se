package io.axoniq.axonserver.enterprise.messaging;

import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class RemoteAxonServerStreamObserver<R, T> implements ClientResponseObserver<R, T> {

    private final StreamObserver<T> delegate;
    private ClientCallStreamObserver<R> requestStream;

    public RemoteAxonServerStreamObserver(StreamObserver<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onNext(T t) {
        delegate.onNext(t);
    }

    @Override
    public void onError(Throwable throwable) {
        StreamObserverUtils.complete(requestStream);

        MessagingPlatformException messagingPlatformException = GrpcExceptionBuilder.parse(throwable);
        delegate.onError(messagingPlatformException);
    }

    @Override
    public void onCompleted() {
        StreamObserverUtils.complete(requestStream);
        delegate.onCompleted();
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<R> requestStream) {
        this.requestStream = requestStream;
    }
}
