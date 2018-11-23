package io.axoniq.axonserver.cluster;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

/**
 * @author Milan Savic
 */
public interface Scheduler {

    ScheduledRegistration schedule(Runnable command, long delay, TimeUnit timeUnit);

    void shutdownNow();

    Clock clock();

    interface ScheduledRegistration extends Registration {

        long getDelay(TimeUnit unit);

        long getElapsed(TimeUnit unit);
    }
}
