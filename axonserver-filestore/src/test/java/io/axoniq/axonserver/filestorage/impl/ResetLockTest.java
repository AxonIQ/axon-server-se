package io.axoniq.axonserver.filestorage.impl;

import junit.framework.TestCase;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ResetLockTest extends TestCase {

    public void testStartReset() {
        ResetLock resetLock = new ResetLock();
        resetLock.increaseActiveRequests();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.schedule(resetLock::decreaseActiveRequests, 250, TimeUnit.MILLISECONDS);
        resetLock.startReset();

        try {
            resetLock.increaseActiveRequests();
            fail("Should not be able to register a new request when reset in progress");
        } catch (FileStoreException ex) {
            assertEquals(FileStoreErrorCode.RESET_IN_PROGRESS, ex.errorCode());
        }

        resetLock.stopReset();
        resetLock.increaseActiveRequests();
        resetLock.increaseActiveRequests();
        resetLock.decreaseActiveRequests();
        resetLock.decreaseActiveRequests();
        resetLock.startReset();
        assertTrue(resetLock.resetting());
    }

    public void testStartResetFails() {
        ResetLock resetLock = new ResetLock();
        resetLock.startReset();
        try {
            resetLock.startReset();
            fail("Should not be able to start a new reset with a reset in progress");
        } catch (FileStoreException ex) {
            assertEquals(FileStoreErrorCode.RESET_IN_PROGRESS, ex.errorCode());
        }
    }
}