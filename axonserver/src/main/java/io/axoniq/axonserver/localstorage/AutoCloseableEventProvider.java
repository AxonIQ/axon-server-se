/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * Provides the event for a specific token using a {@link CloseableIterator} of {@link SerializedEventWithToken}.
 */
public class AutoCloseableEventProvider {

    private static final Logger logger = LoggerFactory.getLogger(AutoCloseableEventProvider.class);

    private final Duration autocloseableDeadline = Duration.ofSeconds(60);
    private final AtomicReference<ScheduledFuture<?>> scheduledDeadline = new AtomicReference<>();
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final AtomicReference<CloseableIterator<SerializedEventWithToken>> iteratorRef = new AtomicReference<>();
    private final Function<Long, CloseableIterator<SerializedEventWithToken>> iteratorFactory;

    /**
     * Constructs an instance base on the specified {@link CloseableIterator} of {@link SerializedEventWithToken}.
     *
     * @param iteratorFactory used to iterate the events in the event store.
     */
    public AutoCloseableEventProvider(Function<Long, CloseableIterator<SerializedEventWithToken>> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    /**
     * Returns a {@link Mono} of the {@link Event} with the specified token.
     *
     * @param token the token of the event to be retrieved
     * @return a {@link Mono} of the {@link Event} with the specified token.
     */
    public Mono<Event> event(long token) {
        return Mono.create(sink -> {
            cancelClosing();
            executorService.submit(() -> {
                try {
                    readEvent(sink, token);
                } catch (Exception t) {
                    logger.error("Error happened while trying to read the event.", t);
                    sink.error(t);
                }
            });
        });
    }

    /**
     * Closes the iterator used to access the event store.
     *
     * @return a {@link Mono} that completes when the close operation is completed.
     */
    public Mono<Void> close() {
        return Mono.fromRunnable(() -> {
            cancelClosing();
            scheduleClosing(0L);
        });
    }

    private void readEvent(MonoSink<Event> sink, long token) {
        logger.info("Reading event for token {}.", token);
        CloseableIterator<SerializedEventWithToken> iterator = iterator(token);

        if (!iterator.hasNext()) {
            logger.info("No event for token {}.", token);
            sink.success();
        } else {
            SerializedEventWithToken next = iterator.next();
            if (next.getToken() > token) {
                iterator = newIterator(token);
                next = iterator.next();
            }

            //todo possible optimization if the gap is very large
            while (iterator.hasNext() && next.getToken() < token) {
                next = iterator.next();
                logger.info("Just read {} event.", next);
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
            iterator = newIterator(token);
        }
        return iterator;
    }

    private CloseableIterator<SerializedEventWithToken> newIterator(long token) {
        CloseableIterator<SerializedEventWithToken> current = iteratorRef.get();
        if (current != null) {
            logger.debug("Closing the open event iterator.");
            current.close();
        }
        logger.debug("Creating a new event iterator starting from token {}", token);
        CloseableIterator<SerializedEventWithToken> iterator = iteratorFactory.apply(token);
        if (!iteratorRef.compareAndSet(current, iterator)) {
            logger.debug("Another thread create the new event iterator. Closing the new one...");
            iterator.close();
            iterator = iteratorRef.get();
        }
        return iterator;
    }
}
