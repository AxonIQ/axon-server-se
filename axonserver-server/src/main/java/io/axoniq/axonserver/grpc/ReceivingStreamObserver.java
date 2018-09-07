package io.axoniq.axonserver.grpc;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

/**
 * Wrapper around a StreamObserver for receiving GRPC messages. Catches exceptions on handling of the message and logs them.
 * This avoids closing the GRPC connection when an exception occurs on processing of the message.
 * Author: marc
 */
public abstract class ReceivingStreamObserver<T> implements StreamObserver<T> {
    private final Logger logger;

    protected ReceivingStreamObserver(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void onNext(T message) {
        if( logger.isTraceEnabled()) logger.trace("{}: Received: {}", sender(), message);
        try {
            consume(message);
        } catch( RuntimeException cause) {
            logger.warn("{}: Execution of command failed: {}", sender(), cause.getMessage());
        }
    }


    protected abstract void consume(T message);

    protected abstract String sender();

}
