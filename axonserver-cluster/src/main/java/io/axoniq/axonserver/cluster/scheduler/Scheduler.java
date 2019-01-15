package io.axoniq.axonserver.cluster.scheduler;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

/**
 * Schedules tasks to be executed in future.
 *
 * @author Milan Savic
 * @since 4.1
 */
public interface Scheduler {

    /**
     * Schedules a {@code command} to be executed after given {@code delay} in given {@code timeUnit}.
     *
     * @param command  the task to be executed
     * @param delay    for how long the command execution is delayed
     * @param timeUnit the time unit
     * @return information about the schedule, and means to cancel it
     */
    ScheduledRegistration schedule(Runnable command, long delay, TimeUnit timeUnit);

    /**
     * Schedules a {@code command} to be executed periodically after {@code initialDelay}. Frequency of execution is
     * represented by {@code delay}. New task is scheduled after execution of current one.
     *
     * @param command      the task to be executed
     * @param initialDelay for how long the first execution is delayed
     * @param delay        frequency of task execution
     * @param timeUnit     the time unit
     * @return information about the schedule, and means to cancel it
     */
    ScheduledRegistration scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit timeUnit);

    /**
     * Cancels all scheduled tasks.
     */
    void shutdownNow();

    /**
     * Gets the clock used by this scheduler.
     *
     * @return the clock
     */
    Clock clock();
}
