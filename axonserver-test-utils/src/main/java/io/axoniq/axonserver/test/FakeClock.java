/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * Clock that allows test cases to set the time.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class FakeClock extends Clock {

    Instant instant;

    public FakeClock() {
        this(Instant.now());
    }

    public FakeClock(Instant instant) {
        this.instant = instant;
    }

    @Override
    public ZoneId getZone() {
        return null;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return instant;
    }

    public void timeElapses(long millis) {
        instant = instant.plusMillis(millis);
    }

    public void timeElapses(long delay, TimeUnit timeUnit) {
        timeElapses(timeUnit.toMillis(delay));
    }
}
