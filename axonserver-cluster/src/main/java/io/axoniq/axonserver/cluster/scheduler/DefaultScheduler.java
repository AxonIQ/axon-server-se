package io.axoniq.axonserver.cluster.scheduler;

import io.axoniq.axonserver.cluster.util.AxonThreadFactory;

import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Implements the {@link Scheduler} by wrapping the {@link ScheduledExecutorService}.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class DefaultScheduler implements Scheduler {

    private final ScheduledExecutorService scheduledExecutorService;
    private final Clock clock = Clock.systemUTC();

    /**
     * Creates the Default Scheduler, uses {@link Executors#newScheduledThreadPool(int)}.
     */
    public DefaultScheduler() {
        this(Executors.newScheduledThreadPool(2));
    }

    /**
     * Creates the Default Scheduler, uses {@link Executors#newScheduledThreadPool(int, ThreadFactory)}.
     *
     * @param threadGroup the prefix used from the thread factory for new threads name.
     */
    public DefaultScheduler(String threadGroup) {
        this(Executors.newScheduledThreadPool(2, new AxonThreadFactory(threadGroup)));
    }

    /**
     * Creates the Default Scheduler using provided {@code scheduledExecutorService}.
     *
     * @param scheduledExecutorService used for scheduling purposes
     */
    public DefaultScheduler(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    public Clock clock() {
        return clock;
    }

    public ScheduledRegistration scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                        TimeUnit timeUnit) {
        ScheduledFuture<?> schedule =
                scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, delay, timeUnit);
        return new DefaultScheduledRegistration(clock, schedule);
    }

    @Override
    public ScheduledRegistration schedule(Runnable command, long delay, TimeUnit timeUnit) {
        ScheduledFuture<?> schedule = scheduledExecutorService.schedule(command, delay, timeUnit);
        return new DefaultScheduledRegistration(clock, schedule);
    }

    @Override
    public void execute(Runnable command) {
        scheduledExecutorService.execute(command);
    }

    @Override
    public void shutdownNow() {
        scheduledExecutorService.shutdownNow();
    }
}
