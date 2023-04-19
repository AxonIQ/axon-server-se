package io.axoniq.axonserver.util;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultIdLockTest {

    private final DefaultIdLock testSubject = new DefaultIdLock();

    @Test
    public void request() {
        IdLock.Ticket result = testSubject.request("demo");
        assertTrue(result.isAcquired());

        IdLock.Ticket second = testSubject.request("demo2");
        assertFalse(second.isAcquired());

        result.release();
        assertFalse(result.isAcquired());

        second = testSubject.request("demo2");
        assertTrue(second.isAcquired());
    }

    @Test
    public void requestMultipleSameKey() {
        IdLock.Ticket result = testSubject.request("demo");
        assertTrue(result.isAcquired());

        IdLock.Ticket second = testSubject.request("demo");
        assertTrue(second.isAcquired());

        result.release();
        assertFalse(result.isAcquired());

        IdLock.Ticket third = testSubject.request("demo2");
        assertFalse(third.isAcquired());

        second.release();
        third = testSubject.request("demo2");
        assertTrue(third.isAcquired());
    }
}