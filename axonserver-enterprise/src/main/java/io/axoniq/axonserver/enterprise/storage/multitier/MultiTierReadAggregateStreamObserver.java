package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.message.event.EventStore;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Utility class to read events for an aggregate for a multi-tier event store.
 * Starts reading events from the leader ({@code responseStreamObserver}). When the first sequence number returned by
 * the leader is not a snapshot and not the first event for the aggregate, the handler sends the same request to the
 * lower tier to get events from there and merges the responses.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class MultiTierReadAggregateStreamObserver implements StreamObserver<SerializedEvent> {

    private final String context;
    private final AtomicBoolean allowSnapshotsFromLowerTier = new AtomicBoolean();
    private final AtomicLong expectedFirst;
    private final GetAggregateEventsRequest request;
    private final StreamObserver<SerializedEvent> responseStreamObserver;
    private final AtomicBoolean canForwardFromLeader = new AtomicBoolean();
    private final List<SerializedEvent> eventsFromLeader = new ArrayList<>();
    private final AtomicBoolean secondaryStarted = new AtomicBoolean();
    private final AtomicBoolean secondaryCompleted = new AtomicBoolean();
    private final AtomicBoolean primaryCompleted = new AtomicBoolean();
    private final Function<String, EventStore> secondaryEventStoreLocator;
    private final AtomicLong snapshotSequenceFromLowerTier = new AtomicLong(-1);

    public MultiTierReadAggregateStreamObserver(String context, GetAggregateEventsRequest request,
                                                StreamObserver<SerializedEvent> responseStreamObserver,
                                                Function<String, EventStore> secondaryEventStoreLocator) {
        this.context = context;
        expectedFirst = new AtomicLong(request.getInitialSequence());
        this.request = request;
        this.allowSnapshotsFromLowerTier.set(request.getAllowSnapshots());
        this.responseStreamObserver = responseStreamObserver;
        this.secondaryEventStoreLocator = secondaryEventStoreLocator;
    }

    @Override
    public void onNext(SerializedEvent serializedEvent) {
        if (!canForwardFromLeader.get()) {
            if (serializedEvent.getAggregateSequenceNumber() == expectedFirst.get() || serializedEvent.asEvent()
                                                                                                      .getSnapshot()) {
                responseStreamObserver.onNext(serializedEvent);
                if (serializedEvent.asEvent().getSnapshot()) {
                    allowSnapshotsFromLowerTier.set(false);
                    expectedFirst.set(serializedEvent.getAggregateSequenceNumber() + 1);
                } else {
                    canForwardFromLeader.set(true);
                }
            } else {
                if (secondaryStarted.compareAndSet(false, true)) {
                    startReadingFromLowerTier(serializedEvent.getAggregateSequenceNumber());
                }
                synchronized (canForwardFromLeader) {
                    if (canForwardFromLeader.get()) {
                        checkAndSend(responseStreamObserver, serializedEvent);
                    } else {
                        eventsFromLeader.add(serializedEvent);
                    }
                }
            }
        } else {
            checkAndSend(responseStreamObserver, serializedEvent);
        }
    }


    private void checkAndSend(StreamObserver<SerializedEvent> responseStreamObserver, SerializedEvent serializedEvent) {
        if (serializedEvent.getAggregateSequenceNumber() > snapshotSequenceFromLowerTier.get()) {
            responseStreamObserver.onNext(serializedEvent);
            snapshotSequenceFromLowerTier.set(-1);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        responseStreamObserver.onError(throwable);
    }

    @Override
    public void onCompleted() {
        primaryCompleted.set(true);
        if (!canForwardFromLeader.get() && !secondaryStarted.get()) {
            startReadingFromLowerTier(Long.MAX_VALUE);
        } else if (canForwardFromLeader.get() || secondaryCompleted.get()) {
            responseStreamObserver.onCompleted();
        }
    }

    private void startReadingFromLowerTier(long maxValue) {
        SecondaryStreamObserver secondaryStreamObserver = new SecondaryStreamObserver(maxValue);
        secondaryStreamObserver.start();
    }

    private class SecondaryStreamObserver implements StreamObserver<SerializedEvent> {

        private final long last;
        private final AtomicBoolean first = new AtomicBoolean(true);

        public SecondaryStreamObserver(long last) {
            this.last = last;
        }

        public void start() {
            secondaryEventStoreLocator.apply(context)
                                      .listAggregateEvents(context,
                                                           GetAggregateEventsRequest.newBuilder(request)
                                                                                    .setInitialSequence(expectedFirst
                                                                                                                .get())
                                                                                    .setAllowSnapshots(
                                                                                            allowSnapshotsFromLowerTier
                                                                                                    .get())
                                                                                    .setMaxSequence(last)
                                                                                    .build(), this);
        }

        @Override
        public void onNext(SerializedEvent event) {
            if (first.compareAndSet(true, false)) {
                // check if first event from lower tier is a snapshot and its sequence nr is higher/equal to first
                // event from primary tier. In this case we must ensure that the primary tier will not send
                // events with sequence lower or equal to this sequence number
                if (event.asEvent().getSnapshot() && event.getAggregateSequenceNumber() >= last) {
                    snapshotSequenceFromLowerTier.set(event.getAggregateSequenceNumber());
                }
                responseStreamObserver.onNext(event);
            } else {
                responseStreamObserver.onNext(event);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            responseStreamObserver.onError(throwable);
        }

        @Override
        public void onCompleted() {
            synchronized (canForwardFromLeader) {
                eventsFromLeader.forEach(e -> checkAndSend(responseStreamObserver, e));
                eventsFromLeader.clear();
                canForwardFromLeader.set(true);
            }
            secondaryCompleted.set(true);
            if (primaryCompleted.get()) {
                responseStreamObserver.onCompleted();
            }
        }
    }
}
