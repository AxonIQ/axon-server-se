package io.axoniq.axonserver.cluster.scheduler;

import java.time.Clock;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Default implementation of {@link ScheduledRegistration}. Uses {@link Clock} to measure time.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class DefaultScheduledRegistration implements ScheduledRegistration {

    private final Clock clock;
    private final Runnable cancelFunction;
    private final Function<TimeUnit, Long> delayFunction;
    private final long startMillis;

    /**
     * Creates the Default Scheduled Registration. Wraps around {@code scheduledFuture}.
     *
     * @param clock           used to measure elapsed time
     * @param scheduledFuture used to cancel the registration and to give information about the delay
     */
    public DefaultScheduledRegistration(Clock clock, ScheduledFuture<?> scheduledFuture) {
        this(clock, () -> scheduledFuture.cancel(true), scheduledFuture::getDelay);
    }

    /**
     * Creates the Default Scheduled Registration.
     *
     * @param clock          used to measure elapsed time
     * @param cancelFunction used to cancel the schedule
     * @param delayFunction  used to give information about the delay
     */
    public DefaultScheduledRegistration(Clock clock, Runnable cancelFunction, Function<TimeUnit, Long> delayFunction) {
        this.clock = clock;
        this.cancelFunction = cancelFunction;
        this.delayFunction = delayFunction;
        this.startMillis = clock.millis();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return delayFunction.apply(unit);
    }

    @Override
    public long getElapsed(TimeUnit unit) {
        long elapsedMillis = clock.millis() - startMillis;
        return unit.convert(elapsedMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void cancel() {
        cancelFunction.run();
    }

    @Override
    public String toString() {
        return "ScheduledRegistration: " + this.hashCode() +
                " Delay: " + getDelay(TimeUnit.MILLISECONDS) +
                " Elapsed: " + getElapsed(TimeUnit.MILLISECONDS);
    }
}
