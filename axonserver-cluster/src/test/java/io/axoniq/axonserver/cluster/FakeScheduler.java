package io.axoniq.axonserver.cluster;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.compare;

/**
 * @author Milan Savic
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

    public FakeScheduler() {
        this(Instant.now());
    }

    public FakeScheduler(Instant currentTime) {
        clock = new FakeClock(currentTime);
    }

    public Registration schedule(Runnable command, long delayInMillis) {
        return schedule(command, delayInMillis, TimeUnit.MILLISECONDS);
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
        return new ScheduledRegistration() {
            @Override
            public long getDelay(TimeUnit unit) {
                long millisLeft = triggerTime.minusMillis(clock.instant().toEpochMilli()).toEpochMilli();
                return unit.convert(millisLeft, TimeUnit.MILLISECONDS);
            }

            @Override
            public long getElapsed(TimeUnit unit) {
                long millisElapsed = timeUnit.toMillis(delay) - getDelay(TimeUnit.MILLISECONDS);
                return unit.convert(millisElapsed, TimeUnit.MILLISECONDS);
            }

            @Override
            public void cancel() {
                tasks.remove(task);
            }

            @Override
            public String toString() {
                return "ScheduledRegistration: " + this.hashCode() +
                        " Delay: " + getDelay(TimeUnit.MILLISECONDS) +
                        " Elapsed: " + getElapsed(TimeUnit.MILLISECONDS);
            }
        };
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                     TimeUnit timeUnit) {
        // TODO: 12/18/2018  
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdownNow() {
        tasks.clear();
    }

    public synchronized Instant nextSchedule() {
        return Optional.ofNullable(tasks.first())
                       .map(first -> first.scheduledTime)
                       .orElse(null);
    }

    public synchronized Instant getCurrentTime() {
        return clock.instant();
    }

    public void timeElapses(long delayInMillis) {
        timeElapses(delayInMillis, TimeUnit.MILLISECONDS);
    }

    public synchronized void timeElapses(long delay, TimeUnit timeUnit) {
        clock.plusMillis(timeUnit.toMillis(delay));
        while (!tasks.isEmpty() && !tasks.first().scheduledTime.isAfter(clock.instant())) {
            tasks.pollFirst().command.run();
        }
    }
}
