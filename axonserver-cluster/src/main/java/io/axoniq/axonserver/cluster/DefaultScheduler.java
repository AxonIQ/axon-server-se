package io.axoniq.axonserver.cluster;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Milan Savic
 */
public class DefaultScheduler implements Scheduler {

    private final ScheduledExecutorService scheduledExecutorService;

    public DefaultScheduler() {
        this(Executors.newSingleThreadScheduledExecutor());
    }

    public DefaultScheduler(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    public Registration schedule(Runnable command, long delay, TimeUnit timeUnit) {
        ScheduledFuture<?> schedule = scheduledExecutorService.schedule(command, delay, timeUnit);
        return () -> schedule.cancel(true);
    }

    @Override
    public void shutdownNow() {
        scheduledExecutorService.shutdownNow();
    }
}
