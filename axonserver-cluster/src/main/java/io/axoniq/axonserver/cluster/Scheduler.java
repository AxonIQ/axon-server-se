package io.axoniq.axonserver.cluster;

import java.util.concurrent.TimeUnit;

/**
 * @author Milan Savic
 */
public interface Scheduler {

    ScheduledRegistration schedule(Runnable command, long delay, TimeUnit timeUnit);

    void shutdownNow();

    interface ScheduledRegistration extends Registration {

        long getDelay(TimeUnit unit);

        long getElapsed(TimeUnit unit);
    }
}
