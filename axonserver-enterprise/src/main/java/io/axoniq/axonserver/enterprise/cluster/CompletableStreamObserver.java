package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Stream observer implementation that maps a stream observer to a completable future. Completable future completes on
 * the first message received.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class CompletableStreamObserver<T, R> implements StreamObserver<T> {


    private final String action;
    private final Logger logger;
    private final CompletableFuture<R> completableFuture;
    private final Function<T, R> converter;

    public CompletableStreamObserver(CompletableFuture<R> completableFuture, String action, Logger logger) {
        //noinspection unchecked
        this(completableFuture, action, logger, result -> (R) result);
    }

    public CompletableStreamObserver(CompletableFuture<R> completableFuture, String action, Logger logger,
                                     Function<T, R> converter) {
        this.completableFuture = completableFuture;
        this.action = action;
        this.logger = logger;
        this.converter = converter;
    }

    @Override
    public void onNext(T t) {
        completableFuture.complete(converter.apply(t));
    }

    @Override
    public void onError(Throwable throwable) {
        logger.warn("{}: Remote action failed", action, throwable);
        completableFuture.completeExceptionally(GrpcExceptionBuilder.parse(throwable));
    }

    @Override
    public void onCompleted() {
        if( !completableFuture.isDone()) completableFuture.complete(null);
    }
}
