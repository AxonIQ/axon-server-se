package io.axoniq.axonserver.cluster.scheduler;

import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
     * Creates the Default Scheduler, uses {@link Executors#newSingleThreadScheduledExecutor()}.
     */
    public DefaultScheduler() {
        this(Executors.newSingleThreadScheduledExecutor());
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
    public void shutdownNow() {
        scheduledExecutorService.shutdownNow();
    }
}
