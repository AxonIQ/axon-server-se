package io.axoniq.axonserver.grpc;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Wrapper around a GRPC StreamObserver that ensures thread safety for sending messages, as GRPC does not provide this by default.
 * @author Marc Gathier
 */
public class SendingStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> delegate;
    private static final Logger logger = LoggerFactory.getLogger(SendingStreamObserver.class);
    private final ReentrantLock guard = new ReentrantLock();

    public SendingStreamObserver(StreamObserver<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onNext(T t) {
        guard.lock();
        try {
            delegate.onNext(t);
        } catch( RuntimeException | OutOfDirectMemoryError e) {
            logger.warn("Error while sending message: {}", e.getMessage());
            try {
                // Cancel RPC
                delegate.onError(e);
            } catch (Throwable ex) {
                // Ignore further exception on cancelling the RPC
            }
        } finally {
            guard.unlock();
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
