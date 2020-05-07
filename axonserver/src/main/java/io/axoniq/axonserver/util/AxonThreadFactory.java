package io.axoniq.axonserver.util;

import org.springframework.util.Assert;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

/**
 * Thread factory that created threads in a given group.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public class AxonThreadFactory implements ThreadFactory {

    private final int priority;
    private final ThreadGroup threadGroup;
    private final AtomicInteger threadNumber = new AtomicInteger();

    /**
     * Initializes a ThreadFactory instance that creates each thread in a group with given {@code groupName} with
     * default priority.
     *
     * @param groupName The name of the group to create each thread in
     * @see Thread#setPriority(int)
     */
    public AxonThreadFactory(String groupName) {
        this(new ThreadGroup(groupName));
    }

    /**
     * Initializes a ThreadFactory instance that create each thread in the given {@code group} with default
     * priority.
     *
     * @param group The ThreadGroup to create each thread in
     * @see Thread#setPriority(int)
     */
    public AxonThreadFactory(ThreadGroup group) {
        this(Thread.NORM_PRIORITY, group);
    }

    /**
     * Initializes a ThreadFactory instance that create each thread in the given {@code group} with given
     * {@code priority}.
     *
     * @param priority The priority of the threads to create
     * @param group    The ThreadGroup to create each thread in
     * @see Thread#setPriority(int)
     */
    public AxonThreadFactory(int priority, ThreadGroup group) {
        Assert.isTrue(priority <= Thread.MAX_PRIORITY && priority >= Thread.MIN_PRIORITY,
                      () -> "Given priority is invalid");
        this.priority = priority;
        this.threadGroup = group;
    }

    @Override
    public Thread newThread(@Nonnull Runnable r) {
        Thread thread = new Thread(threadGroup, r, threadGroup.getName() + "-" + nextThreadNumber());
        thread.setPriority(priority);
        return thread;
    }

    private int nextThreadNumber() {
        return threadNumber.getAndIncrement();
    }
}
