package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * @author Marc Gathier
 */
public class CompletableStreamObserver<T, R> implements StreamObserver<T> {


     private final Logger logger;
    private final CompletableFuture<R> completableFuture;
    private final Function<T, R> converter;

    public CompletableStreamObserver(CompletableFuture<R> completableFuture, Logger logger) {
        this(completableFuture, logger, result -> (R)result);
    }

    public CompletableStreamObserver(CompletableFuture<R> completableFuture, Logger logger, Function<T,R> converter) {
        this.completableFuture = completableFuture;
        this.logger = logger;
        this.converter = converter;
    }

    @Override
    public void onNext(T t) {
        completableFuture.complete(converter.apply(t));
    }

    @Override
    public void onError(Throwable throwable) {
        logger.warn("Remote action failed", throwable);
        completableFuture.completeExceptionally(GrpcExceptionBuilder.parse(throwable));
    }

    @Override
    public void onCompleted() {
        if( !completableFuture.isDone()) completableFuture.complete(null);
    }
}
