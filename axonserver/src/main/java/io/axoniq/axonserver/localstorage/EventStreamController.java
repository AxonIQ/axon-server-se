package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public class EventStreamController {
    private static final Executor threadPool = Executors.newCachedThreadPool(new CustomizableThreadFactory("event-stream-"));
    private static final Logger logger = LoggerFactory.getLogger(EventStreamController.class);
    private final Consumer<EventWithToken> eventWithTokenConsumer;
    private final StorageCallback storageCallback;
    private final EventStore datafileManagerChain;
    private final EventWriteStorage eventWriteStorage;
    private final AtomicLong remainingPermits = new AtomicLong();
    private final AtomicLong currentTrackingToken = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean processingBacklog = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Registration eventListener;

    public EventStreamController(
            Consumer<EventWithToken> eventWithTokenConsumer,
            Consumer<Throwable> errorCallback, EventStore datafileManagerChain, EventWriteStorage eventWriteStorage) {
        this.eventWithTokenConsumer = eventWithTokenConsumer;
        this.storageCallback = new StorageCallback() {
            @Override
            public boolean onCompleted(long firstToken) {
                processingBacklog.set(false);
                logger.debug("Done processing backlog at: {}", currentTrackingToken.get());

                return false;
            }

            @Override
            public void onError(Throwable cause) {
                errorCallback.accept(cause);
            }
        };
        this.datafileManagerChain = datafileManagerChain;
        this.eventWriteStorage = eventWriteStorage;
    }

    public void update(long trackingToken, long numberOfPermits) {
        currentTrackingToken.compareAndSet(Long.MIN_VALUE, trackingToken);
        long oldRemaining = remainingPermits.getAndAdd(numberOfPermits);
        if( oldRemaining == 0) {
            threadPool.execute(this::startTracker);
        }
    }

    private void startTracker() {
        try {
            if( processingBacklog.compareAndSet(false, true) ) {
                logger.info("Start tracker from token: {}", currentTrackingToken);
                cancelListener();
                running.set(true);
                this.eventListener = eventWriteStorage.registerEventListener(this::sendFromWriter);
                datafileManagerChain.streamEvents(currentTrackingToken.get(), storageCallback,
                                                  this::sendFromStream);

            }
        } catch(Exception ex) {
            logger.warn("Failed to stream", ex);
            cancelListener();
            storageCallback.onError(ex);
        }
    }

    private void sendFromWriter(EventWithToken eventWithToken) {
        long current = currentTrackingToken.get();
        if(current > eventWithToken.getToken()) return;
        if( processingBacklog.get()) {
            return;
        }

        int retries = 20;
        while( current != eventWithToken.getToken() && retries > 0) {
            logger.debug("Received unexpected token: {} while expecting: {}", eventWithToken.getToken(), current);
            LockSupport.parkNanos(100);
            current = currentTrackingToken.get();
            retries--;
        }

        if(current != eventWithToken.getToken()) {
            threadPool.execute(this::startTracker);
            return;
        }

        sendEvent(eventWithToken);
    }

    private boolean sendFromStream(EventWithToken eventWithToken) {
        if( eventWriteStorage.getLastToken() < eventWithToken.getToken()) return false;
        return sendEvent(eventWithToken);
    }

    private boolean sendEvent(EventWithToken eventWithToken) {
        if( ! running.get() ) return false;
        long claimsLeft = remainingPermits.getAndDecrement();
        if( claimsLeft <= 0) {
            remainingPermits.incrementAndGet();
            cancelListener();
            return false;
        }

        eventWithTokenConsumer.accept(eventWithToken);
        currentTrackingToken.incrementAndGet();
        return true;
    }

    private void cancelListener() {
        if( eventListener != null) {
            eventListener.cancel();
            eventListener = null;
        }
    }


    public void stop() {
        running.set(false);
        cancelListener();
    }
}
