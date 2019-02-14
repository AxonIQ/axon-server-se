package io.axoniq.axonserver.enterprise.cluster;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * @author Marc Gathier
 */
public class CompletableStreamObserver<T, R> implements StreamObserver<T> {


    private final CompletableFuture<R> completableFuture;
    private final Function<T, R> converter;

    public CompletableStreamObserver(CompletableFuture<R> completableFuture) {
        this(completableFuture, result -> (R)result);
    }

    public CompletableStreamObserver(CompletableFuture<R> completableFuture, Function<T,R> converter) {
        this.completableFuture = completableFuture;
        this.converter = converter;
    }

    @Override
    public void onNext(T t) {
        completableFuture.complete(converter.apply(t));
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
