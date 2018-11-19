package io.axoniq.axonserver.cluster;

import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

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
            return scheduledTime.compareTo(other.scheduledTime);
        }
    }

    private NavigableSet<ScheduledTask> tasks = new TreeSet<>();
    private Instant currentTime;

    public FakeScheduler() {
        this(Instant.now());
    }

    public FakeScheduler(TemporalAccessor currentTime) {
        this.currentTime = Instant.from(currentTime);
    }

    public Registration schedule(Runnable command, long delayInMillis) {
        return schedule(command, delayInMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized ScheduledRegistration schedule(Runnable command, long delay, TimeUnit timeUnit) {
        Instant triggerTime = currentTime.plusMillis(timeUnit.toMillis(delay));
        ScheduledTask task = new ScheduledTask(command, triggerTime);
        tasks.add(task);
        return new ScheduledRegistration() {
            @Override
            public long getDelay(TimeUnit unit) {
                long millisLeft = triggerTime.minusMillis(currentTime.toEpochMilli()).toEpochMilli();
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
        };
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
        return currentTime;
    }

    public void timeElapses(long delayInMillis) {
        timeElapses(delayInMillis, TimeUnit.MILLISECONDS);
    }

    public synchronized void timeElapses(long delay, TimeUnit timeUnit) {
        currentTime = currentTime.plusMillis(timeUnit.toMillis(delay));
        while (!tasks.isEmpty() && !tasks.first().scheduledTime.isAfter(currentTime)) {
            tasks.pollFirst().command.run();
        }
    }
}
