/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.message.command.WrappedCommand;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Sinks;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class CommandStream {

    private static final Logger logger = LoggerFactory.getLogger(CommandStream.class);

    private final Sinks.Many<WrappedCommand> listener;
    private final PriorityBlockingQueue<WrappedCommand> queue;
    private final AtomicReference<Subscription> subscription = new AtomicReference<>();
    private final AtomicLong permits = new AtomicLong();
    private final int hardLimit;

    public CommandStream(StreamObserver<SerializedCommandProviderInbound> wrappedResponseObserver, int softLimit) {
        this.hardLimit = (int)Math.floor(softLimit*1.1d);
        this.queue = new PriorityBlockingQueue<WrappedCommand>(10, Comparator.comparing(WrappedCommand::priority)) {
            @Override
            public boolean offer(@Nonnull WrappedCommand command) {
                logger.warn("Offer: {}", command.command().getMessageIdentifier());
                int size = size();
                if( size >= softLimit) {
                    if (size >= hardLimit || command.priority() <=  0) {
                        return false;
                    }
                }
                return super.offer(command);
            }
        };
        this.listener = Sinks.many().unicast().onBackpressureBuffer(queue);
        this.listener.asFlux()
                     .doOnCancel(() -> StreamObserverUtils.complete(wrappedResponseObserver))
                     .subscribe(new Subscriber<WrappedCommand>() {

                         @Override
                         public void onSubscribe(Subscription subscription) {
                             CommandStream.this.subscription.set(subscription);
                         }

                         @Override
                         public void onNext(WrappedCommand wrappedCommand) {
                             SerializedCommandProviderInbound request =
                                     SerializedCommandProviderInbound.newBuilder()
                                                                     .setCommand(wrappedCommand.command())
                                                                     .build();
                             wrappedResponseObserver.onNext(request);
                             permits.decrementAndGet();
                         }

                         @Override
                         public void onError(Throwable throwable) {
                             wrappedResponseObserver.onError(throwable);
                         }

                         @Override
                         public void onComplete() {
                             wrappedResponseObserver.onCompleted();
                         }
                     });
    }


    public List<WrappedCommand> cancel() {
        List<WrappedCommand> queued = new LinkedList<>();
        queue.drainTo(queued);
        subscription.get().cancel();
        return queued;
    }

    public void addPermits(long permits) {
        this.permits.addAndGet(permits);
        subscription.get().request(permits);
    }

    public List<WrappedCommand> emitError(Exception exception) {
        listener.tryEmitError(exception);
        List<WrappedCommand> queued = new LinkedList<>();
        queue.drainTo(queued);
        return queued;
    }

    public void emitNext(WrappedCommand wrappedCommand, Sinks.EmitFailureHandler failureHandler) {
        listener.emitNext(wrappedCommand, failureHandler);
    }

    public int waiting() {
        return queue.size();
    }

    public long permits() {
        return permits.get();
    }
}
