/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.PayloadDescription;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final Logger logger = LoggerFactory.getLogger(TrackingEventProcessorManager.class);

    private final ScheduledExecutorService scheduledExecutorService;
    private final Set<EventTracker> eventTrackerSet = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean replicationRunning = new AtomicBoolean();
    private final String context;
    private final Function<Long, CloseableIterator<SerializedEventWithToken>> iteratorBuilder;
    private final int blacklistedSendAfter;

    /**
     * Constructor for {@link TrackingEventProcessorManager}.
     * @param eventStorageEngine the event storage engine
     * @param blacklistedSendAfter max number of ignored events before sending next event
     */
    public TrackingEventProcessorManager(EventStorageEngine eventStorageEngine, int blacklistedSendAfter) {
        this(eventStorageEngine.getType().getContext(), eventStorageEngine::getGlobalIterator, blacklistedSendAfter);
    }

    /**
     * Constructor for {@link TrackingEventProcessorManager} for easier testing.
     * @param context the context for the storage engine
     * @param iteratorBuilder function that creates an event iterator
     * @param blacklistedSendAfter max number of ignored events before sending next event
     */
    TrackingEventProcessorManager(String context, Function<Long, CloseableIterator<SerializedEventWithToken>> iteratorBuilder, int blacklistedSendAfter) {
        this.context = context;
        this.iteratorBuilder = iteratorBuilder;
        // Use 2 threads (one to send events and one to avoid queuing of reschedules.
        this.scheduledExecutorService = Executors.newScheduledThreadPool(2, new CustomizableThreadFactory(
                context + "-trackers-"));
        this.blacklistedSendAfter = blacklistedSendAfter;
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
                for (EventTracker eventTracker : eventTrackerSet) {
                    try {
                        sent += eventTracker.sendNext();
                    } catch (Throwable ex) {
                        failedReplicators.add(eventTracker);
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
     * Creates a new event tracker.
     *
     * @param trackingToken          the tracking token to start tracking events from
     * @param clientId               the id of the client
     * @param forceReadingFromLeader whether reading events from leader is forced
     * @param eventStream            the output stream
     * @return an EventTracker
     */
    EventTracker createEventTracker(long trackingToken, String clientId, boolean forceReadingFromLeader,
                                    StreamObserver<SerializedEventWithToken> eventStream) {
        return new EventTracker(trackingToken, clientId, forceReadingFromLeader, eventStream);
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
     * Stops all tracking event processors where request does not allow reading from follower.
     */
    public void stopAllWhereNotAllowedReadingFromFollower() {
        eventTrackerSet.forEach(EventTracker::stopAllWhereNotAllowedReadingFromFollower);
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
        private final AtomicInteger permits = new AtomicInteger();
        /**
         * Keeps next token to send to client.
         */
        private final AtomicLong nextToken;
        /**
         * Keeps timestamp when the tracker last ran out of permits.
         */
        private final AtomicLong lastPermitTimestamp;
        private final StreamObserver<SerializedEventWithToken> eventStream;
        private final String client;
        private volatile CloseableIterator<SerializedEventWithToken> eventIterator;
        private volatile boolean running = true;
        private final Set<PayloadDescription> blacklistedTypes = new CopyOnWriteArraySet<>();
        private volatile int force = blacklistedSendAfter;
        private final boolean forceReadingFromLeader;

        private EventTracker(long trackingToken, String clientId, boolean forceReadingFromLeader,
                             StreamObserver<SerializedEventWithToken> eventStream) {
            client = clientId;
            lastPermitTimestamp = new AtomicLong(System.currentTimeMillis());
            nextToken = new AtomicLong(trackingToken);
            this.eventStream = eventStream;
            this.forceReadingFromLeader = forceReadingFromLeader;
        }

        private int sendNext() {
            if (!running) {
                StreamObserverUtils.complete(eventStream);
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     context + ":Tracking event processor stopped for " + client);
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
                    SerializedEventWithToken next = eventIterator.next();
                    if( !blacklisted(next)) {
                        eventStream.onNext(next);
                        if (permits.decrementAndGet() == 0) {
                            lastPermitTimestamp.set(System.currentTimeMillis());
                        }
                        force = blacklistedSendAfter;
                    } else {
                        force--;
                    }
                    count++;
                }
            } catch (IllegalStateException ex) {
                // closed during iterating events
            } catch (Exception ex) {
                close();
                sendError(ex);
            }

            return count;
        }

        private boolean blacklisted(SerializedEventWithToken next) {
            return force > 1 && !blacklistedTypes.isEmpty() && blacklistedTypes.contains(payloadType(next));
        }

        private PayloadDescription payloadType(SerializedEventWithToken next) {
            return PayloadDescription.newBuilder().
                    setRevision(next.asEvent().getPayload().getRevision()).setType(next.asEvent().getPayload().getType()).
                    build();
        }

        private void sendError(Exception ex) {
            StreamObserverUtils.error(eventStream, ex);
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

        public void start() {
            eventTrackerSet.add(this);
            reschedule();
        }

        public void stop() {
            close();
            StreamObserverUtils.complete(eventStream);
        }

        public void stopAllWhereNotAllowedReadingFromFollower() {
            if (forceReadingFromLeader) {
                stop();
            }
        }

        public void validateActiveConnection(long minLastPermits) {
            if (permits.get() > 0) {
                return;
            }

            if (lastPermitTimestamp.get() < minLastPermits) {
                close();
                sendError(new MessagingPlatformException(ErrorCode.OTHER,
                                                         context + ": Timeout waiting for permits from client "
                                                                 + client));
            }
        }

        public void addBlacklist(List<PayloadDescription> blacklistList) {
            if (logger.isDebugEnabled()) {
                blacklistList.forEach(i -> logger.debug("{}: Blacklisting: {} for {}", context, i, client));
            }
            blacklistedTypes.addAll(blacklistList);
        }
    }
}
