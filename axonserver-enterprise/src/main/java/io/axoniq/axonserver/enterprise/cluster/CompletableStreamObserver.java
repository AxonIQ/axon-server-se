package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

/**
 * Author: marc
 */
public class CompletableStreamObserver implements StreamObserver<Confirmation> {

    private final CompletableFuture<Void> completableFuture;

    public CompletableStreamObserver(CompletableFuture<Void> completableFuture) {
        this.completableFuture = completableFuture;
    }

    @Override
    public void onNext(Confirmation t) {
        completableFuture.complete(null);
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
