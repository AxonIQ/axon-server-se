package io.axoniq.axonhub.grpc;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper around a GRPC StreamObserver that ensures thread safety for sending messages, as GRPC does not provide this by default.
 * Author: marc
 */
public class SendingStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> delegate;
    private static final Logger logger = LoggerFactory.getLogger(SendingStreamObserver.class);
    private final AtomicBoolean guard = new AtomicBoolean(true);

    public SendingStreamObserver(StreamObserver<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onNext(T t) {
        while( ! guard.compareAndSet(true, false)) {
            // busy wait, no action
        }
        try {
            delegate.onNext(t);
        } finally {
            guard.set(true);
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
