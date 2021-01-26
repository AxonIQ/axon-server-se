package io.axoniq.axonserver.localstorage;

import io.grpc.stub.CallStreamObserver;

/**
 * @author Milan Savic
 */
public  class CallStreamObserverDelegator<T> extends CallStreamObserver<T>  {

    private final CallStreamObserver<T> delegate;

    public CallStreamObserverDelegator(CallStreamObserver<T> delegate) {
        this.delegate = delegate;
    }

    protected CallStreamObserver<T> delegate(){
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
