package io.axoniq.axonserver.localstorage.file;


import org.junit.*;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.junit.Assert.*;


/**
 * Unit tests for StandardIndexEntries
 *
 * @author Sara Pellegrini
 */
public class StandardIndexEntriesTest {

    @Test
    public void testRangeDuringWriting() throws InterruptedException {
        LinkedList<Integer> list = new LinkedList<>();
        StandardIndexEntries testSubject = new StandardIndexEntries(0, list);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch started = new CountDownLatch(1);
        new Thread(() -> {
            while (testSubject.size() < 10000) {
                if (testSubject.size() == 100) {
                    started.countDown();
                }
                int index = counter.getAndIncrement();
                testSubject.addAll(Collections.singletonList(new IndexEntry(index, index, index)));
            }
            running.set(false);
        }).start();

        started.await();
        while (running.get()) {
            IndexEntries range = testSubject.range(0, testSubject.size(), false);
            Integer expectedPosition = 0;
            for (Integer position : range.positions()) {
                assertEquals(expectedPosition, position);
                expectedPosition = expectedPosition + 1;
            }
            System.out.println(expectedPosition);
        }
        running.set(false);
    }

    @Test
    public void addAndLoopPerformance() {
        StandardIndexEntries standardIndexEntries = new StandardIndexEntries(10);
        standardIndexEntries.last();
        long before = System.currentTimeMillis();
        IntStream.range(0, 200_000).forEach(i -> standardIndexEntries.add(new IndexEntry(1, i, 1)));
        assertTrue("Added entries - " + (System.currentTimeMillis() - before),
                   System.currentTimeMillis() - before < 250);
        AtomicLong total = new AtomicLong();
        before = System.currentTimeMillis();
        standardIndexEntries.positions().forEach(total::addAndGet);
        assertTrue("Iterate entries - " + (System.currentTimeMillis() - before),
                   System.currentTimeMillis() - before < 250);
        Assert.assertEquals(1998, (int) standardIndexEntries.positions().get(1998));
        Assert.assertEquals(4000, (int) standardIndexEntries.positions().get(4000));
    }
}