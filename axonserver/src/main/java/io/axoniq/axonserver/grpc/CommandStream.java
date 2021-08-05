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
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Sinks;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class CommandStream {

    private static final Logger logger = LoggerFactory.getLogger(CommandStream.class);

    private final Sinks.Many<WrappedCommand> sink;
    private final PriorityBlockingQueue<WrappedCommand> queue;
    private final AtomicLong permits = new AtomicLong();
    private final int hardLimit;
    private final BaseSubscriber<WrappedCommand> subscriber;

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
        this.sink = Sinks.many().unicast().onBackpressureBuffer(queue);
        this.subscriber = new BaseSubscriber<WrappedCommand>() {

            @Override
            protected void hookOnNext(@Nonnull WrappedCommand wrappedCommand) {
                SerializedCommandProviderInbound request =
                        SerializedCommandProviderInbound.newBuilder()
                                                        .setCommand(wrappedCommand.command())
                                                        .build();
                wrappedResponseObserver.onNext(request);
                permits.decrementAndGet();
            }

            @Override
            protected void hookOnComplete() {
                wrappedResponseObserver.onCompleted();
            }

            @Override
            protected void hookOnError(@Nonnull Throwable throwable) {
                wrappedResponseObserver.onError(throwable);
            }

            @Override
            protected void hookOnCancel() {
                wrappedResponseObserver.onCompleted();
            }
        };
        this.sink.asFlux()
                 .subscribe(subscriber);
    }


    public List<WrappedCommand> cancel() {
        List<WrappedCommand> queued = new LinkedList<>();
        queue.drainTo(queued);
        subscriber.cancel();
        return queued;
    }

    public void addPermits(long permits) {
        this.permits.addAndGet(permits);
        subscriber.request(permits);
    }

    public List<WrappedCommand> emitError(Exception exception) {
        sink.tryEmitError(exception);
        List<WrappedCommand> queued = new LinkedList<>();
        queue.drainTo(queued);
        return queued;
    }

    public void emitNext(WrappedCommand wrappedCommand, Sinks.EmitFailureHandler failureHandler) {
        sink.emitNext(wrappedCommand, failureHandler);
    }

    public int waiting() {
        return queue.size();
    }

    public long permits() {
        return permits.get();
    }
}
