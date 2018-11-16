package io.axoniq.axonserver.cluster;

import java.util.concurrent.TimeUnit;

/**
 * @author Milan Savic
 */
public interface Scheduler {

    Registration schedule(Runnable command, long delay, TimeUnit timeUnit);

    void shutdownNow();
}
