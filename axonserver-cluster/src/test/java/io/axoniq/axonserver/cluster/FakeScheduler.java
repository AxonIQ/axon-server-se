package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.scheduler.DefaultScheduledRegistration;
import io.axoniq.axonserver.cluster.scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;

import java.time.Clock;
import java.time.Instant;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Integer.compare;
import static java.time.Duration.between;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * {@link Scheduler} implementation for test purposes. It allows for manual moving time forward.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class FakeScheduler implements Scheduler {

    private class ScheduledTask implements Comparable<ScheduledTask> {

        private final Runnable command;
        private final Instant scheduledTime;

        private ScheduledTask(Runnable command, Instant scheduledTime) {
            this.command = command;
            this.scheduledTime = scheduledTime;
        }

        @Override
        public int compareTo(ScheduledTask other) {
            int compareTime = scheduledTime.compareTo(other.scheduledTime);
            return compareTime != 0 ? compareTime : compare(command.hashCode(), other.command.hashCode());
        }
    }

    private NavigableSet<ScheduledTask> tasks = new TreeSet<>();
    private FakeClock clock;

    /**
     * Instantiates the Fake Scheduler with {@link Instant#now()} time moment.
     */
    public FakeScheduler() {
        this(Instant.now());
    }

    /**
     * Instantiates the Fake Scheduler with given {@code currentTime} time moment.
     *
     * @param currentTime current time this scheduler is aware of
     */
    public FakeScheduler(Instant currentTime) {
        clock = new FakeClock(currentTime);
    }

    @Override
    public Clock clock() {
        return clock;
    }

    @Override
    public synchronized ScheduledRegistration schedule(Runnable command, long delay, TimeUnit timeUnit) {
        Instant triggerTime = clock.instant().plusMillis(timeUnit.toMillis(delay));
        ScheduledTask task = new ScheduledTask(command, triggerTime);
        tasks.add(task);
        return new DefaultScheduledRegistration(clock,
                                                () -> tasks.remove(task),
                                                unit -> getDelay(triggerTime, unit));
    }

    @Override
    public ScheduledRegistration scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                        TimeUnit timeUnit) {
        AtomicReference<ScheduledRegistration> registration = new AtomicReference<>();
        Runnable runnable = new Runnable() {

            private Instant next = clock.instant().plusMillis(timeUnit.toMillis(initialDelay));

            @Override
            public void run() {
                command.run();
                next = next.plusMillis(timeUnit.toMillis(delay));
                registration.set(schedule(this, between(clock.instant(), next).toMillis(), MILLISECONDS));
            }
        };

        registration.set(schedule(runnable, initialDelay, timeUnit));
        return new DefaultScheduledRegistration(clock, registration.get()::cancel, registration.get()::getDelay );
    }

    @Override
    public void shutdownNow() {
        tasks.clear();
    }

    /**
     * Gives information about next schedule.
     *
     * @return {@link Instant} when next task is scheduled
     */
    public synchronized Instant nextSchedule() {
        return Optional.ofNullable(tasks.first())
                       .map(first -> first.scheduledTime)
                       .orElse(null);
    }

    /**
     * Gives information about current time.
     *
     * @return {@link Instant} of current time
     */
    public synchronized Instant getCurrentTime() {
        return clock.instant();
    }

    /**
     * Moves time forward for given {@code delayInMillis} millis.
     *
     * @param delayInMillis for how long to move the time wheel in millis
     */
    public void timeElapses(long delayInMillis) {
        timeElapses(delayInMillis, MILLISECONDS);
    }

    /**
     * Moves time forward for given {@code delay} in given {@code timeUnit}.
     *
     * @param delay    for how long to move the time wheel in {@code timeUnit}
     * @param timeUnit time unit
     */
    public synchronized void timeElapses(long delay, TimeUnit timeUnit) {
        clock.plusMillis(timeUnit.toMillis(delay));
        while (!tasks.isEmpty() && !tasks.first().scheduledTime.isAfter(clock.instant())) {
            tasks.pollFirst().command.run();
        }
    }

    private Long getDelay(Instant triggerTime, TimeUnit unit) {
        long millisLeft = triggerTime.minusMillis(clock.instant().toEpochMilli()).toEpochMilli();
        return unit.convert(millisLeft, TimeUnit.MILLISECONDS);
    }
}
