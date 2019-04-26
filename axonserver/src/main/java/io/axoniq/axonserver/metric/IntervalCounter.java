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
import java.util.concurrent.atomic.LongAdder;

/**
 * @author Marc Gathier
 */
public class IntervalCounter extends SlidingWindow<AtomicInteger> {
    private LongAdder total = new LongAdder();

    public IntervalCounter(Clock clock) {
        super(1, 16*60, TimeUnit.SECONDS, AtomicInteger::new, (i1,i2) -> new AtomicInteger(i1.get()+ i2.get()), clock);
    }

    public int getOneMinuteRate() {
        return reduce(1, TimeUnit.MINUTES).get()/60;
    }

    public int getFiveMinuteRate() {
        return reduce(5, TimeUnit.MINUTES).get()/300;
    }

    public int getFifteenMinuteRate() {
        return reduce(15, TimeUnit.MINUTES).get()/900;
    }

    public void mark() {
        current().incrementAndGet();
        total.add(1);
    }

    public long count() {
        return total.longValue();
    }
}
