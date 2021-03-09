package io.axoniq.axonserver.grpc.stream;

import io.grpc.stub.CallStreamObserver;

/**
 * Utility class used to simplify the implementation of a {@link CallStreamObserver} based on a delegate
 * {@link CallStreamObserver}. Extending this class, all the delegated methods are inherited.
 *
 * @author Milan Savic
 * @author Sara Pellegrini
 * @see io.axoniq.axonserver.message.event.ForwardingStreamObserver
 */
public abstract class CallStreamObserverDelegator<T> extends CallStreamObserver<T> {

    private final CallStreamObserver<T> delegate;

    protected CallStreamObserverDelegator(CallStreamObserver<T> delegate) {
        this.delegate = delegate;
    }

    protected CallStreamObserver<T> delegate() {
        return delegate;
    }

    public boolean isReady() {
        return this.delegate().isReady();
    }

    public void setOnReadyHandler(Runnable runnable) {
        this.delegate().setOnReadyHandler(runnable);
    }

    public void disableAutoInboundFlowControl() {
        this.delegate().disableAutoInboundFlowControl();
    }

    public void request(int i) {
        this.delegate().request(i);
    }

    public void setMessageCompression(boolean b) {
        this.delegate().setMessageCompression(b);
    }

    public void onNext(T t) {
        this.delegate().onNext(t);
    }

    public void onError(Throwable throwable) {
        this.delegate().onError(throwable);
    }

    public void onCompleted() {
        this.delegate().onCompleted();
    }

}
