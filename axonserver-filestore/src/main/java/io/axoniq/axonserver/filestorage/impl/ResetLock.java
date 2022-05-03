package io.axoniq.axonserver.filestorage.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class to ensure that resetting a file store can only be started if there are no more active operations.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public class ResetLock {

    private final static Logger logger = LoggerFactory.getLogger(ResetLock.class);
    private final AtomicReference<LockInfo> lockInfo = new AtomicReference<>(new LockInfo(0, false, null));

    /**
     * Resets the lock information and sends error to a waiting process (if available).
     */
    public void clear() {
        LockInfo old = lockInfo.getAndSet(new LockInfo(0, false, null));
        if (old.waiting != null) {
            old.waiting.emitError(new IllegalStateException("Reset action cancelled"), Sinks.EmitFailureHandler.FAIL_FAST);
        }
    }

    void increaseActiveRequests() {
        lockInfo.updateAndGet(old -> {
            if (old.resetting) {
                throw new FileStoreException(FileStoreErrorCode.RESET_IN_PROGRESS, "Reset in progress");
            }
            return new LockInfo(old.activeRequests + 1, false, old.waiting);
        });
    }

    void decreaseActiveRequests() {
        LockInfo info = lockInfo.updateAndGet(old -> new LockInfo(old.activeRequests - 1, old.resetting, old.waiting));

        if (info.activeRequests == 0 && info.waiting != null) {
            info.waiting.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
        }
    }

    void startReset() {
        LockInfo info = lockInfo.updateAndGet(old -> {
            if (old.resetting) {
                throw new FileStoreException(FileStoreErrorCode.RESET_IN_PROGRESS, "Reset in progress");
            }
            return new LockInfo(old.activeRequests, true, Sinks.one());
        });
        logger.debug("Start reset. {} pending actions", info.activeRequests);
        if (info.activeRequests == 0) {
            info.waiting.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
        }
        info.waiting.asMono().block();
        lockInfo.updateAndGet(current -> new LockInfo(current.activeRequests, true, null));
        logger.debug("Start reset. Continue");
    }

    void stopReset() {
        LockInfo old = lockInfo.getAndUpdate(current -> new LockInfo(current.activeRequests, false, null));
        if (old.waiting != null) {
            old.waiting.emitError(new IllegalStateException("Reset action cancelled"), Sinks.EmitFailureHandler.FAIL_FAST);
        }
    }

    boolean resetting() {
        return lockInfo.get().resetting;
    }

    private static class LockInfo {

        private final int activeRequests;
        private final boolean resetting;
        private final Sinks.One<Void> waiting;

        private LockInfo(int activeRequests, boolean resetting, Sinks.One<Void> waiting) {
            this.activeRequests = activeRequests;
            this.resetting = resetting;
            this.waiting = waiting;
        }
    }
}
