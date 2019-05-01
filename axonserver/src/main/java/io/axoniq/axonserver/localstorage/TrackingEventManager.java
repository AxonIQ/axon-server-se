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
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.util.ReadyStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Manages all tracking event processors for a single context.
 *
 * @author Marc Gathier
 * @since 4.1.2
 *
 */
public class TrackingEventManager {

    private static final int MAX_EVENTS_PER_RUN = 500;
    private final ScheduledExecutorService scheduledExecutorService;
    private final EventStorageEngine eventStorageEngine;
    private final Set<EventTracker> eventTrackerSet = new CopyOnWriteArraySet<>();
    private final AtomicBoolean replicationRunning = new AtomicBoolean();
    private final AtomicBoolean reschedule = new AtomicBoolean(true);
    private final Logger logger = LoggerFactory.getLogger(TrackingEventManager.class);

    public TrackingEventManager(EventStorageEngine eventStorageEngine) {
        this.eventStorageEngine = eventStorageEngine;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new CustomizableThreadFactory(
                "trackers-" + eventStorageEngine.getType().getContext()));
        this.scheduledExecutorService.schedule(this::sendEvents, 100, TimeUnit.MILLISECONDS);
    }

    private void sendEvents() {
        if (!replicationRunning.compareAndSet(false, true)) {
            // it's fine, replication is already in progress
            return;
        }
        try {
            int runsWithoutChanges = 0;
            List<EventTracker> failedReplicators = new ArrayList<>();
            while (runsWithoutChanges < 3) {
                int sent = 0;
                failedReplicators.clear();
                for (EventTracker raftPeer : eventTrackerSet) {
                    try {
                        sent += raftPeer.sendNext();
                    } catch (Throwable ex) {
                        failedReplicators.add(raftPeer);
                    }
                }
                if (!failedReplicators.isEmpty()) {
                    logger.warn("{}: removing {} replicators",
                                eventStorageEngine.getType().getContext(),
                                failedReplicators.size());
                    eventTrackerSet.removeAll(failedReplicators);
                    logger.warn("{}: {} replicators remaining",
                                eventStorageEngine.getType().getContext(),
                                eventTrackerSet.size());
                }

                if (sent == 0) {
                    runsWithoutChanges++;
                } else {
                    runsWithoutChanges = 0;
                }
            }
        } finally {
            if( reschedule.get()) {
                scheduledExecutorService.schedule(this::sendEvents,
                                                  100,
                                                  MILLISECONDS);
                replicationRunning.set(false);
            }
        }
    }


    EventTracker createEventTracker(GetEventsRequest request, ReadyStreamObserver<InputStream> eventStream) {
        EventTracker eventTracker = new EventTracker(request, eventStream);
        eventTrackerSet.add(eventTracker);
        reschedule();
        return eventTracker;
    }

    public void reschedule() {
    }

    public void stopAll() {
        reschedule.set(false);
        eventTrackerSet.forEach(EventTracker::stop);
        scheduledExecutorService.shutdown();
    }

    public void validateActiveConnections(long minLastPermits) {
        eventTrackerSet.forEach(t -> t.validateActiveConnection(minLastPermits));
    }

    /**
     * Handles one event stream to a client.
     */
    public class EventTracker {

        /**
         * Keeps number of permits still available.
         */
        private final AtomicInteger permits;
        /**
         * Keeps next token to send to client.
         */
        private final AtomicLong nextToken;
        /**
         * Keeps timestamp when the tracker last ran out of permits.
         */
        private final AtomicLong lastPermitTimestamp;
        private final ReadyStreamObserver<InputStream> eventStream;
        private volatile CloseableIterator<SerializedEventWithToken> eventIterator;
        private volatile boolean running = true;

        private EventTracker(GetEventsRequest request, ReadyStreamObserver<InputStream> eventStream) {
            permits = new AtomicInteger((int) request.getNumberOfPermits());
            lastPermitTimestamp = new AtomicLong(System.currentTimeMillis());
            nextToken = new AtomicLong(request.getTrackingToken());
            this.eventStream = eventStream;
        }

        private int sendNext() {
            if (!running) {
                throw new RuntimeException("tracker is closed");
            }
            if (eventIterator == null) {
                eventIterator = eventStorageEngine.getGlobalIterator(nextToken.get());
            }

            int count = 0;
            try {
                while (running
                        && permits.get() > 0
                        && count < MAX_EVENTS_PER_RUN
                        && eventIterator.hasNext()
                        && eventStream.isReady()
                ) {
                    eventStream.onNext(eventIterator.next().asInputStream());
                    if (permits.decrementAndGet() == 0) {
                        lastPermitTimestamp.set(System.currentTimeMillis());
                    }
                    count++;
                }
            } catch (Exception ex) {
                running = false;
                sendError(ex);
            }

            return count;
        }

        private void sendError(Exception ex) {
            try {
                eventStream.onError(GrpcExceptionBuilder.build(ex));
            } catch (Exception ignoredException) {
                // ignore
            }
        }

        public void addPermits(int newPermits) {
            permits.addAndGet(newPermits);
            reschedule();
        }

        public void close() {
            running = false;
            if (eventIterator != null) {
                eventIterator.close();
                eventIterator = null;
            }
        }

        public void stop() {
            close();
            sendError(new MessagingPlatformException(ErrorCode.NO_LEADER_AVAILABLE, "Leader stepped down"));
        }

        public void validateActiveConnection(long minLastPermits) {
            if (permits.get() > 0) {
                return;
            }

            if (lastPermitTimestamp.get() < minLastPermits) {
                close();
                sendError(new MessagingPlatformException(ErrorCode.OTHER, "Timeout waiting for permits from client"));
            }
        }
    }
}
