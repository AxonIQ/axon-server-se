package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.message.event.EventStore;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Utility class to read snapshots for an aggregate for a multi-tier event store.
 * Starts reading snapshots from the leader ({@code responseStreamObserver}). When the first sequence number returned by
 * the leader is not the first expected snapshot for the aggregate, the handler sends the same request to the
 * lower tier to get snapshots from there and merges the responses.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class MultiTierReadSnapshotStreamObserver implements StreamObserver<SerializedEvent> {

    private final String context;
    private final AtomicLong expectedFirst;
    private final GetAggregateSnapshotsRequest request;
    private final StreamObserver<SerializedEvent> responseStreamObserver;
    private final AtomicBoolean canForwardFromLeader = new AtomicBoolean();
    private final List<SerializedEvent> eventsFromLeader = new ArrayList<>();
    private final AtomicBoolean secondaryStarted = new AtomicBoolean();
    private final AtomicBoolean secondaryCompleted = new AtomicBoolean();
    private final AtomicBoolean primaryCompleted = new AtomicBoolean();
    private final Function<String, EventStore> secondaryEventStoreLocator;

    public MultiTierReadSnapshotStreamObserver(String context, GetAggregateSnapshotsRequest request,
                                               StreamObserver<SerializedEvent> responseStreamObserver,
                                               Function<String, EventStore> secondaryEventStoreLocator) {
        this.context = context;
        expectedFirst = new AtomicLong(request.getInitialSequence());
        this.request = request;
        this.responseStreamObserver = responseStreamObserver;
        this.secondaryEventStoreLocator = secondaryEventStoreLocator;
    }

    @Override
    public void onNext(SerializedEvent serializedEvent) {
        if (!canForwardFromLeader.get()) {
            if (serializedEvent.getAggregateSequenceNumber() <= expectedFirst.get()) {
                if (serializedEvent.getAggregateSequenceNumber() == expectedFirst.get()) {
                    responseStreamObserver.onNext(serializedEvent);
                }
                canForwardFromLeader.set(true);
            } else {
                if (secondaryStarted.compareAndSet(false, true)) {
                    startReadingFromLowerTier(serializedEvent.getAggregateSequenceNumber());
                }
                synchronized (canForwardFromLeader) {
                    if (canForwardFromLeader.get()) {
                        responseStreamObserver.onNext(serializedEvent);
                    } else {
                        eventsFromLeader.add(serializedEvent);
                    }
                }
            }
        } else {
            responseStreamObserver.onNext(serializedEvent);
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
        } else if (secondaryCompleted.get()) {
            responseStreamObserver.onCompleted();
        }
    }

    private void startReadingFromLowerTier(long maxValue) {
        SecondaryStreamObserver secondaryStreamObserver = new SecondaryStreamObserver(maxValue);
        secondaryStreamObserver.start();
    }

    private class SecondaryStreamObserver implements StreamObserver<SerializedEvent> {

        private final long last;

        public SecondaryStreamObserver(long last) {
            this.last = last;
        }

        public void start() {
            GetAggregateSnapshotsRequest lowerTierRequest = GetAggregateSnapshotsRequest.newBuilder(request)
                                                                                        .setInitialSequence(
                                                                                                expectedFirst.get())
                                                                                        .build();
            secondaryEventStoreLocator.apply(context).listAggregateSnapshots(context, lowerTierRequest, this);
        }

        @Override
        public void onNext(SerializedEvent serializedEvent) {
            if (serializedEvent.getAggregateSequenceNumber() < last) {
                responseStreamObserver.onNext(serializedEvent);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            responseStreamObserver.onError(throwable);
        }

        @Override
        public void onCompleted() {
            synchronized (canForwardFromLeader) {
                eventsFromLeader.forEach(e -> responseStreamObserver.onNext(e));
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
