/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class AutoCloseableEventProvider {

    private final Duration autocloseableDeadline = Duration.ofSeconds(60);
    private final AtomicReference<ScheduledFuture<?>> scheduledDeadline = new AtomicReference<>();
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final AtomicReference<CloseableIterator<SerializedEventWithToken>> iteratorRef = new AtomicReference<>();
    private final Function<Long, CloseableIterator<SerializedEventWithToken>> iteratorFactory;

    public AutoCloseableEventProvider(Function<Long, CloseableIterator<SerializedEventWithToken>> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    public Mono<Event> event(long token) {
        return Mono.create(sink -> {
            cancelClosing();
            executorService.submit(() -> readEvent(sink, token));
        });
    }

    public Mono<Void> close() {
        return Mono.fromRunnable(() -> {
            cancelClosing();
            scheduleClosing(0L);
        });
    }

    private void readEvent(MonoSink<Event> sink, long token) {
        CloseableIterator<SerializedEventWithToken> iterator = iterator(token);

        if (!iterator.hasNext()) {
            sink.success();
        } else {
            SerializedEventWithToken next = iterator.next();
            if (next.getToken() > token) {
                iterator = newIterator(iterator, token);
                next = iterator.next();
            }

            //todo possible optimization if the gap is very large
            while (iterator.hasNext() && next.getToken() < token) {
                next = iterator.next();
            }

            if (next.getToken() == token) {
                sink.success(next.asEvent());
            } else {
                sink.success();
            }
        }
        scheduleClosing();
    }

    private void cancelClosing() {
        ScheduledFuture<?> scheduledFuture = scheduledDeadline.get();
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    private void scheduleClosing() {
        scheduleClosing(autocloseableDeadline.toMillis());
    }

    private void scheduleClosing(long milliseconds) {
        ScheduledFuture<?> schedule = executorService.schedule(() -> {
            CloseableIterator<SerializedEventWithToken> i = iteratorRef.get();
            iteratorRef.set(null);
            if (i != null) {
                i.close();
            }
        }, milliseconds, TimeUnit.MILLISECONDS);
        scheduledDeadline.set(schedule);
    }

    private CloseableIterator<SerializedEventWithToken> iterator(long token) {
        CloseableIterator<SerializedEventWithToken> iterator = iteratorRef.get();
        if (iterator == null || !iterator.hasNext()) {
            iterator = newIterator(null, token);
        }
        return iterator;
    }

    private CloseableIterator<SerializedEventWithToken> newIterator(CloseableIterator<SerializedEventWithToken> current,
                                                                    long token) {
        if (current != null) {
            current.close();
        }
        CloseableIterator<SerializedEventWithToken> iterator = iteratorFactory.apply(token);
        if (!iteratorRef.compareAndSet(current, iterator)) {
            iterator.close();
            iterator = iteratorRef.get();
        }
        return iterator;
    }
}
