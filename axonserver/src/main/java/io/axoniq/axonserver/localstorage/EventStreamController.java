package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
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
    private static final Executor threadPool = Executors.newCachedThreadPool(new CustomizableThreadFactory("event-stream-"){
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = super.newThread(runnable);
            thread.setDaemon(true);
            return thread;
        }
    });
    private static final Logger logger = LoggerFactory.getLogger(EventStreamController.class);
    private final Consumer<SerializedEventWithToken> eventWithTokenConsumer;
    private final Consumer<Throwable> errorCallback;
    private final EventStore datafileManagerChain;
    private final EventWriteStorage eventWriteStorage;
    private final AtomicLong remainingPermits = new AtomicLong();
    private final AtomicLong currentTrackingToken = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean processingBacklog = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Registration eventListener;

    public EventStreamController(
            Consumer<SerializedEventWithToken> eventWithTokenConsumer,
            Consumer<Throwable> errorCallback, EventStore datafileManagerChain, EventWriteStorage eventWriteStorage) {
        this.eventWithTokenConsumer = eventWithTokenConsumer;
        this.errorCallback = errorCallback;
        this.datafileManagerChain = datafileManagerChain;
        this.eventWriteStorage = eventWriteStorage;
    }

    public void update(long trackingToken, long numberOfPermits) {
        currentTrackingToken.compareAndSet(Long.MIN_VALUE, trackingToken);
        if( remainingPermits.getAndAdd(numberOfPermits) <= 0)
            threadPool.execute(this::startTracker);
    }

    // always run async so that calling thread is not blocked by this method
    private void startTracker() {
        try {
            if( remainingPermits.get() > 0 && processingBacklog.compareAndSet(false, true) ) {
                logger.info("Start tracker from token: {}", currentTrackingToken);
                cancelListener();
                running.set(true);
                while( running.get() && remainingPermits.get() > 0 && ! datafileManagerChain.streamEvents(currentTrackingToken.get(),
                                                  this::sendFromStream) ) {
                    if( remainingPermits.get() > 0) {
                        logger.info("restart tracker from token: {}, remaining permits after run {}",
                                    currentTrackingToken,
                                    remainingPermits.get());
                    }
                }

                this.eventListener = eventWriteStorage.registerEventListener(this::sendFromWriter);
                processingBacklog.set(false);
                logger.debug("Done processing backlog at: {}", currentTrackingToken.get());
            }
        } catch(Exception ex) {
            processingBacklog.set(false);
            logger.warn("Failed to stream", ex);
            cancelListener();
            errorCallback.accept(ex);
        }
    }

    private void sendFromWriter(SerializedEventWithToken eventWithToken) {
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

    private boolean sendFromStream(SerializedEventWithToken eventWithToken) {
        if( eventWriteStorage.getLastToken() < eventWithToken.getToken()) return false;
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

        eventWithTokenConsumer.accept(eventWithToken);
        currentTrackingToken.incrementAndGet();
        return true;
    }

    private void cancelListener() {
        if( eventListener != null) {
            eventListener.cancel();
            running.set(false);
            eventListener = null;
        }
    }


    public void stop() {
        running.set(false);
        cancelListener();
    }

    public void cancel() {
        cancelListener();
        errorCallback.accept(new MessagingPlatformException(ErrorCode.OTHER, "Connection reset by server"));
    }

}
