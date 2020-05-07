package io.axoniq.axonserver.util;

import org.axonframework.common.AxonThreadFactory;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class AxonThreadFactoryTest {

    private org.axonframework.common.AxonThreadFactory testSubject;

    @Test
    public void testCreateWithThreadGroupByName() {
        testSubject = new org.axonframework.common.AxonThreadFactory("test");
        Thread t1 = testSubject.newThread(new NoOpRunnable());
        Thread t2 = testSubject.newThread(new NoOpRunnable());

        assertEquals("test", t1.getThreadGroup().getName());
        assertEquals("test-0", t1.getName());
        assertEquals("test-1", t2.getName());
        assertSame("Expected only a single ThreadGroup", t1.getThreadGroup(), t2.getThreadGroup());
    }

    @Test
    public void testCreateWithThreadGroupByThreadGroupInstance() {
        ThreadGroup threadGroup = new ThreadGroup("test");
        testSubject = new org.axonframework.common.AxonThreadFactory(threadGroup);
        Thread t1 = testSubject.newThread(new NoOpRunnable());
        Thread t2 = testSubject.newThread(new NoOpRunnable());

        assertEquals("test", t1.getThreadGroup().getName());
        assertEquals("test-0", t1.getName());
        assertSame("Expected only a single ThreadGroup", threadGroup, t1.getThreadGroup());
        assertSame("Expected only a single ThreadGroup", threadGroup, t2.getThreadGroup());
    }

    @Test
    public void testCreateWithPriority() {
        ThreadGroup threadGroup = new ThreadGroup("test");
        testSubject = new org.axonframework.common.AxonThreadFactory(Thread.MAX_PRIORITY, threadGroup);
        Thread t1 = testSubject.newThread(new NoOpRunnable());
        Thread t2 = testSubject.newThread(new NoOpRunnable());

        assertEquals("test", t1.getThreadGroup().getName());
        assertEquals(Thread.MAX_PRIORITY, t1.getPriority());
        assertSame("Expected only a single ThreadGroup", threadGroup, t1.getThreadGroup());
        assertSame("Expected only a single ThreadGroup", threadGroup, t2.getThreadGroup());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsTooHighPriority() {
        new org.axonframework.common.AxonThreadFactory(Thread.MAX_PRIORITY + 1, new ThreadGroup(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsTooLowPriority() {
        new AxonThreadFactory(Thread.MIN_PRIORITY - 1, new ThreadGroup(""));
    }

    private static class NoOpRunnable implements Runnable {

        @Override
        public void run() {
        }
    }
}