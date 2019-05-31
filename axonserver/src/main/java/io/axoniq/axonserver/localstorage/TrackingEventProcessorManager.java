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
import io.grpc.stub.StreamObserver;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Manages all tracking event processors for a single context.
 *
 * @author Marc Gathier
 * @since 4.1.2
 */
public class TrackingEventProcessorManager {

    private static final int MAX_EVENTS_PER_RUN = 500;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Set<EventTracker> eventTrackerSet = new CopyOnWriteArraySet<>();
    private final AtomicBoolean replicationRunning = new AtomicBoolean();
    private final Logger logger = LoggerFactory.getLogger(TrackingEventProcessorManager.class);
    private final String context;
    private final Function<Long, CloseableIterator<SerializedEventWithToken>> iteratorBuilder;

    public TrackingEventProcessorManager(EventStorageEngine eventStorageEngine) {
        this(eventStorageEngine.getType().getContext(), eventStorageEngine::getGlobalIterator);
    }

    TrackingEventProcessorManager(String context, Function<Long, CloseableIterator<SerializedEventWithToken>> iteratorBuilder) {
        this.context = context;
        this.iteratorBuilder = iteratorBuilder;
        // Use 2 threads (one to send events and one to avoid queuing of reschedules.
        this.scheduledExecutorService = Executors.newScheduledThreadPool(2, new CustomizableThreadFactory(
                "trackers-" + context));
    }

    /**
     * Send events to all tracking event processors until there are no new events or no tracking event processors ready to
     * receive events. If there are tracking event processors left after processing one run, it reschedules to try again
     * after 100ms.
     * Only one instance of this operation will run.
     */
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
                    logger.debug("{}: removing {} replicators",
                                context,
                                failedReplicators.size());
                    eventTrackerSet.removeAll(failedReplicators);
                    logger.debug("{}: {} replicators remaining",
                                context,
                                eventTrackerSet.size());
                }

                if (sent == 0) {
                    runsWithoutChanges++;
                } else {
                    runsWithoutChanges = 0;
                }
            }
        } finally {
            replicationRunning.set(false);
            if (!eventTrackerSet.isEmpty()) {
                scheduledExecutorService.schedule(this::sendEvents,
                                                  100,
                                                  MILLISECONDS);
            }
        }
    }


    /**
     * Registers a new event tracker and starts sending events.
     * @param request the initial request to start the tracking event processor
     * @param eventStream the output stream
     * @return an EventTracker
     */
    EventTracker createEventTracker(GetEventsRequest request, StreamObserver<InputStream> eventStream) {
        EventTracker eventTracker = new EventTracker(request, eventStream);
        eventTrackerSet.add(eventTracker);
        reschedule();
        return eventTracker;
    }

    /**
     * Starts the sendEvents operation if it is not running.
     */
    public void reschedule() {
        if (!replicationRunning.get()) {
            this.scheduledExecutorService.execute(this::sendEvents);
        }
    }

    /**
     * Stop all tracking event processors.
     */
    public void stopAll() {
        eventTrackerSet.forEach(EventTracker::stop);
    }

    /**
     * Kills all tracking event processors that are waiting for permits and not received any permits since minLastPermits.
     * @param minLastPermits expected minimum timestamp for new permits request
     */
    public void validateActiveConnections(long minLastPermits) {
        eventTrackerSet.forEach(t -> t.validateActiveConnection(minLastPermits));
    }

    /**
     * Cleans up manager by stopping all tracking event processors and stopping the scheduling service.
     */
    public void close() {
        stopAll();
        scheduledExecutorService.shutdown();
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
        private final StreamObserver<InputStream> eventStream;
        private volatile CloseableIterator<SerializedEventWithToken> eventIterator;
        private volatile boolean running = true;

        private EventTracker(GetEventsRequest request, StreamObserver<InputStream> eventStream) {
            permits = new AtomicInteger((int) request.getNumberOfPermits());
            lastPermitTimestamp = new AtomicLong(System.currentTimeMillis());
            nextToken = new AtomicLong(request.getTrackingToken());
            this.eventStream = eventStream;
        }

        private int sendNext() {
            if (!running) {
                throw new MessagingPlatformException(ErrorCode.OTHER, "Tracking event processor stopped");
            }
            if (eventIterator == null) {
                eventIterator = iteratorBuilder.apply(nextToken.get());
            }

            int count = 0;
            try {
                while (running
                        && permits.get() > 0
                        && count < MAX_EVENTS_PER_RUN
                        && eventIterator.hasNext()
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
            sendError(new MessagingPlatformException(ErrorCode.OTHER, "Tracking event processor stopped by server"));
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
