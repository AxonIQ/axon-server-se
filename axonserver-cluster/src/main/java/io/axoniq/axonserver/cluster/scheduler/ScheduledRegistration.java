package io.axoniq.axonserver.cluster.scheduler;

import io.axoniq.axonserver.cluster.Registration;

import java.util.concurrent.TimeUnit;

/**
 * Extends registration with scheduled information. It gives how much time has passed since the schedule, and how much
 * time is left.
 *
 * @author Milan Savic
 * @since 4.1
 */
public interface ScheduledRegistration extends Registration {

    /**
     * How much time is left until the schedule is triggered in given {@code unit}.
     *
     * @param unit time unit in which the result is reported
     * @return time left until the schedule is triggered
     */
    long getDelay(TimeUnit unit);

    /**
     * How much time has elapsed since the schedule in given {@code unit}.
     *
     * @param unit time unit in which the result is reported
     * @return time left since the schedule
     */
    long getElapsed(TimeUnit unit);

    /**
     * Cancels the scheduled registration.
     *
     * @param mayInterruptIfRunning {@code true} if the thread executing this task should be interrupted; otherwise,
     *                              in-progress tasks are allowed to complete
     */
    void cancel(boolean mayInterruptIfRunning);
}
