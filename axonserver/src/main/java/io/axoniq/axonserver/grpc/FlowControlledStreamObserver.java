package io.axoniq.axonserver.grpc;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Version of StreamObserver that has notion of number of messages it may send based on the number of permits.
 * When it tries to send a message while out of permits it throws an exception.
 * @author Sara Pellegrini
 */
public class FlowControlledStreamObserver<T> implements StreamObserver<T> {

    private final Logger logger = LoggerFactory.getLogger(FlowControlledStreamObserver.class);

    private final StreamObserver<T> delegate;

    private final AtomicLong permitsLeft = new AtomicLong();

    private final Consumer<Throwable> errorHandler;

    public FlowControlledStreamObserver(StreamObserver<T> delegate,
                                        Consumer<Throwable> errorHandler) {
        this.delegate = delegate;
        this.errorHandler = errorHandler;
    }

    @Override
    public void onNext(T value) {
        if (permitsLeft.getAndDecrement() > 0) {
            synchronized (delegate) {
                delegate.onNext(value);
            }
        } else {
            errorHandler.accept(new IllegalStateException("Zero remaining permits"));
        }
    }

    @Override
    public void onError(Throwable t) {
        delegate.onError(t);
    }

    @Override
    public void onCompleted() {
        delegate.onCompleted();
    }

    public void addPermits(long count){
        long old = permitsLeft.getAndUpdate(o -> Math.max(0, o) + count);
        logger.debug("Adding {} permits, #permits was: {}", count, old);
    }
}
