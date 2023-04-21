package io.axoniq.axonserver.util;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
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

    @Test
    public void concurrentRequest() throws InterruptedException {
        int concurrency = 20;
        ScheduledExecutorService service = Executors.newScheduledThreadPool(concurrency);
        CountDownLatch countDownLatch = new CountDownLatch(concurrency);
        AtomicInteger acquired = new AtomicInteger();
        for (int i = 0; i < concurrency; i++) {
            int id = i;
            service.schedule(() -> {
                IdLock.Ticket ticket = testSubject.request("demo" + id);
                if (ticket.isAcquired()) {
                    acquired.incrementAndGet();
                }
                countDownLatch.countDown();
            }, 1L, TimeUnit.MILLISECONDS);
        }
        countDownLatch.await(1, TimeUnit.SECONDS);
        assertEquals(1, acquired.get());
    }
}