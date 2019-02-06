package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

/**
 * Author: marc
 */
public class CompletableStreamObserver implements StreamObserver<Confirmation> {

    private final CompletableFuture<Void> completableFuture;
    private final Logger logger;

    public CompletableStreamObserver(CompletableFuture<Void> completableFuture, Logger logger) {
        this.completableFuture = completableFuture;
        this.logger = logger;
    }

    @Override
    public void onNext(Confirmation t) {
        completableFuture.complete(null);
    }

    @Override
    public void onError(Throwable throwable) {
        logger.warn("Remote action failed", throwable);
        completableFuture.completeExceptionally(throwable);
    }

    @Override
    public void onCompleted() {
        if( !completableFuture.isDone()) completableFuture.complete(null);
    }
}
