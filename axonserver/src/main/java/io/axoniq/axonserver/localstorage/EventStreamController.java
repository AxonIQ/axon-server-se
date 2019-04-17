/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Handles streams of events from EventStore to client. One instance per tracking event processor.
 * @author Marc Gathier
 */
public class EventStreamController {
    private static final Logger logger = LoggerFactory.getLogger(EventStreamController.class);
    private final Consumer<SerializedEventWithToken> eventWithTokenConsumer;
    private final Consumer<Throwable> errorCallback;
    private final EventStorageEngine datafileManagerChain;
    private final Function<Consumer<SerializedEventWithToken>, Registration> liveEventRegistrationFunction;
    private final EventStreamExecutor eventStreamExecutor;
    private final AtomicLong remainingPermits = new AtomicLong();
    private final AtomicLong currentTrackingToken = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean processingBacklog = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Registration eventListener;
    private final AtomicLong lastMessageSent = new AtomicLong(System.currentTimeMillis());
    private volatile long lastPermitTimestamp;
    private AtomicReference<CloseableIterator<SerializedEventWithToken>> eventIteratorReference = new AtomicReference<>();


    /**
     * Monitor used to synchronize event dispatching to client
     */
    private final Object sendEventMonitor = new Object();

    /**
     * Constructor for the {@link EventStreamController}.
     * @param eventWithTokenConsumer consumer for events
     * @param errorCallback called when there is an error while sending events
     * @param eventStorageEngine storage engine that contains the events
     * @param liveEventRegistrationFunction function to register the controller with the event writer to receive updates
     * @param eventStreamExecutor thread pool to start reading events from event store asynchronously
     */
    public EventStreamController(
            Consumer<SerializedEventWithToken> eventWithTokenConsumer,
            Consumer<Throwable> errorCallback, EventStorageEngine eventStorageEngine,
            Function<Consumer<SerializedEventWithToken>, Registration> liveEventRegistrationFunction,
            EventStreamExecutor eventStreamExecutor) {
        this.eventWithTokenConsumer = eventWithTokenConsumer;
        this.errorCallback = errorCallback;
        this.datafileManagerChain = eventStorageEngine;
        this.liveEventRegistrationFunction = liveEventRegistrationFunction;
        this.eventStreamExecutor = eventStreamExecutor;
    }

    public void update(long trackingToken, long numberOfPermits) {
        currentTrackingToken.compareAndSet(Long.MIN_VALUE, trackingToken);
        lastPermitTimestamp = System.currentTimeMillis();
        if( remainingPermits.getAndAdd(numberOfPermits) <= 0)
            eventStreamExecutor.execute(this::startTracker);
    }

    public boolean missingNewPermits(long minLastPermits) {
        if (remainingPermits.get() > 0) return false;
        return (lastPermitTimestamp < minLastPermits);
    }

    // always run async so that calling thread is not blocked by this method
    private void startTracker() {
        try {
            if( remainingPermits.get() > 0 && processingBacklog.compareAndSet(false, true) ) {
                logger.info("Start tracker from token: {}", currentTrackingToken);
                cancelListener();
                eventIteratorReference.compareAndSet(null, datafileManagerChain.getGlobalIterator(currentTrackingToken.get()));
                running.set(true);
                while( running.get() && remainingPermits.get() > 0 && eventIteratorReference.get().hasNext()) {
                    SerializedEventWithToken serializedEventWithToken = eventIteratorReference.get().next();
                    if( ! sendFromStream(serializedEventWithToken)) break;

                }
                processingBacklog.set(false);

                if( remainingPermits.get() > 0) {
                    closeIterator();
                }
                this.eventListener = liveEventRegistrationFunction.apply(this::sendFromWriter);
                logger.info("Done processing backlog at: {}", currentTrackingToken.get());
            }
        } catch(Throwable ex) {
            processingBacklog.set(false);
            logger.warn("Failed to stream", ex);
            cancelListener();
            errorCallback.accept(ex);
        }
    }

    private void sendFromWriter(SerializedEventWithToken eventWithToken) {
        try {
            long current = currentTrackingToken.get();
            if (current > eventWithToken.getToken()) return;
            if( processingBacklog.get()) {
                return;
            }

            if( remainingPermits.get() > 0 && !sendEvent(eventWithToken)) {
                eventStreamExecutor.execute(this::startTracker);
            }
        } catch (Throwable t) {
            logger.warn("Failed to send {}", eventWithToken, t);
            cancelListener();
            errorCallback.accept(t);
        }
    }

    private boolean sendFromStream(SerializedEventWithToken eventWithToken) {
        if( datafileManagerChain.getLastToken() < eventWithToken.getToken()) return false;
        return sendEvent(eventWithToken);
    }

    private boolean sendEvent(SerializedEventWithToken eventWithToken) {
        if( ! running.get() ) return false;
        long claimsLeft = remainingPermits.getAndDecrement();
        if( claimsLeft <= 0) {
            remainingPermits.incrementAndGet();
            cancelListener();
            return false;
        }

        if (claimsLeft < 5) lastPermitTimestamp = System.currentTimeMillis();

        synchronized (sendEventMonitor) {
            boolean newToken = currentTrackingToken.compareAndSet(eventWithToken.getToken(), eventWithToken.getToken() + 1);
            if (newToken) {
                eventWithTokenConsumer.accept(eventWithToken);
                lastMessageSent.updateAndGet(current -> Math.max(current, System.currentTimeMillis()));
            } else {
                // return permit, concurrent sending attempt
                remainingPermits.incrementAndGet();
            }
            return newToken;
        }
    }

    private void cancelListener() {
        if( eventListener != null) {
            eventListener.cancel();
            running.set(false);
            eventListener = null;
        }
    }

    private void closeIterator() {
        eventIteratorReference.updateAndGet(old -> { if( old != null) old.close();
            return null;}
        );
    }


    public void stop() {
        running.set(false);
        closeIterator();
        cancelListener();
    }

    public void cancel() {
        cancelListener();
        closeIterator();
        errorCallback.accept(new MessagingPlatformException(ErrorCode.OTHER, "Connection reset by server"));
    }
}
