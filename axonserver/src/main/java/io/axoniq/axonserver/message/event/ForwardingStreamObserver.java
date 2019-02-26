package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

/**
 * @author Marc Gathier
 */
public class ForwardingStreamObserver<T> implements StreamObserver<T> {

    private final Logger logger;
    private final String request;
    private final StreamObserver<T> responseObserver;

    public ForwardingStreamObserver(
            Logger logger, String request, StreamObserver<T> responseObserver) {
        this.logger = logger;
        this.request = request;
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(T t) {
        responseObserver.onNext(t);
    }

    @Override
    public void onError(Throwable cause) {
        logger.warn(EventDispatcher.ERROR_ON_CONNECTION_FROM_EVENT_STORE, request, cause.getMessage());
        responseObserver.onError(GrpcExceptionBuilder.build(cause));
    }

    @Override
    public void onCompleted() {
        responseObserver.onCompleted();
    }
}
