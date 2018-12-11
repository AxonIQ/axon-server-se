package io.axoniq.axonserver.enterprise.cluster;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

/**
 * Author: marc
 */
public class CompletableStreamObserver<T> implements StreamObserver<T> {

    private final CompletableFuture<T> completableFuture;

    public CompletableStreamObserver(CompletableFuture<T> completableFuture) {
        this.completableFuture = completableFuture;
    }

    @Override
    public void onNext(T t) {
        completableFuture.complete(t);
    }

    @Override
    public void onError(Throwable throwable) {
        completableFuture.completeExceptionally(throwable);
    }

    @Override
    public void onCompleted() {
        if( !completableFuture.isDone()) completableFuture.complete(null);
    }
}
