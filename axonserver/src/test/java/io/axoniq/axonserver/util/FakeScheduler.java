package io.axoniq.axonserver.util;

import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Integer.compare;
import static java.time.Duration.between;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * {@link ScheduledExecutorService} implementation for test purposes. It allows for manual moving time forward.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class FakeScheduler implements ScheduledExecutorService {

    private static class ScheduledTask implements Comparable<ScheduledTask> {

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

        public void run() {
            command.run();
        }
    }

    private NavigableSet<ScheduledTask> tasks = new TreeSet<>();
    private ChangeableClock clock;

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
        clock = new ChangeableClock(currentTime);
    }

    public int tasks() {
        return tasks.size();
    }

    public Clock clock() {
        return clock;
    }

    @NotNull
    @Override
    public synchronized FakeScheduledRegistration<?> schedule(@NotNull Runnable command, long delay,
                                                              TimeUnit timeUnit) {
        Instant triggerTime = clock.instant().plusMillis(timeUnit.toMillis(delay));
        ScheduledTask task = new ScheduledTask(command, triggerTime);
        tasks.add(task);
        return new FakeScheduledRegistration<>(task);
    }

    @NotNull
    @Override
    public <V> ScheduledFuture<V> schedule(@NotNull Callable<V> callable, long delay, @NotNull TimeUnit unit) {
        throw new NotImplementedException("schedule");
    }

    @NotNull
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command, long initialDelay, long period,
                                                  @NotNull TimeUnit unit) {
        throw new NotImplementedException("scheduleAtFixedRate");
    }

    @NotNull
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command, long initialDelay, long delay,
                                                     TimeUnit timeUnit) {
        AtomicReference<FakeScheduledRegistration<?>> registration = new AtomicReference<>();
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
        return new FakeScheduledRegistration<>(null);
    }

    @Override
    public void execute(Runnable command) {
        command.run();
    }

    @Override
    public void shutdown() {
        tasks.clear();
    }

    @NotNull
    @Override
    public List<Runnable> shutdownNow() {
        tasks.clear();
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) {
        return true;
    }

    @NotNull
    @Override
    public <T> Future<T> submit(@NotNull Callable<T> task) {
        throw new NotImplementedException("submit");
    }

    @NotNull
    @Override
    public <T> Future<T> submit(@NotNull Runnable task, T result) {
        throw new NotImplementedException("submit");
    }

    @NotNull
    @Override
    public Future<?> submit(@NotNull Runnable task) {
        throw new NotImplementedException("submit");
    }

    @NotNull
    @Override
    public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks) {
        throw new NotImplementedException("invokeAll");
    }

    @NotNull
    @Override
    public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks, long timeout,
                                         @NotNull TimeUnit unit) {
        throw new NotImplementedException("invokeAll");
    }

    @NotNull
    @Override
    public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks) {
        throw new NotImplementedException("invokeAny");
    }

    @Override
    public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks, long timeout, @NotNull TimeUnit unit) {
        throw new NotImplementedException("invokeAny");
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
        clock.add(delay, timeUnit);
        while (!tasks.isEmpty() && !tasks.first().scheduledTime.isAfter(clock.instant())) {
            tasks.pollFirst().run();
        }
    }

    private class FakeScheduledRegistration<T> implements ScheduledFuture<T> {

        private final ScheduledTask task;

        public FakeScheduledRegistration(ScheduledTask task) {
            this.task = task;
        }

        @Override
        public long getDelay(@NotNull TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(@NotNull Delayed o) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            tasks.remove(task);
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public T get() {
            return null;
        }

        @Override
        public T get(long timeout, @NotNull TimeUnit unit) {
            return null;
        }
    }
}
