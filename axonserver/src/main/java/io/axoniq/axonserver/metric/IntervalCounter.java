/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counter to determine the rates of an event.
 * @author Marc Gathier
 * @since 4.2
 */
public class IntervalCounter extends SlidingWindow<AtomicInteger> {
    private AtomicLong total = new AtomicLong();
    private final long started;

    public IntervalCounter(Clock clock) {
        super(1, 16*60, TimeUnit.SECONDS, AtomicInteger::new, (i1,i2) -> new AtomicInteger(i1.get()+ i2.get()), clock);
        started = clock.millis();
    }

    /**
     * Calculates the average rate of the last minute. If the meter is running for less than a minute it returns the rate
     * over the time it is running.
     * @return average rate of the last minute
     */
    public int getOneMinuteRate() {
        return aggregate(1, TimeUnit.MINUTES).get()/seconds(60);
    }

    /**
     * Calculates the average rate of the last five minutes. If the meter is running for less than 5 minutes it returns the rate
     * over the time it is running.
     * @return average rate of the last five minutes
     */
    public int getFiveMinuteRate() {
        return aggregate(5, TimeUnit.MINUTES).get()/seconds(300);
    }

    /**
     * Calculates the average rate of the last 15 minutes. If the meter is running for less than 15 minutes it returns the rate
     * over the time it is running.
     * @return average rate of the last 15 minutes
     */
    public int getFifteenMinuteRate() {
        return aggregate(15, TimeUnit.MINUTES).get()/seconds(900);
    }

    private int seconds(int i) {
        return (int)Math.min(i, Math.max(1,(clock.millis() - started)/1000));
    }

    /**
     * Register one occurrence of the event.
     */
    public void mark() {
        current().incrementAndGet();
        total.incrementAndGet();
    }

    /**
     * Returns the total number of event since counter created.
     * @return the total number of event since counter created
     */
    public long count() {
        return total.longValue();
    }
}
