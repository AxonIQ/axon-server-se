/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
 */
public class ChangeableClock extends Clock {

    Instant instant;

    public ChangeableClock() {
        this(Instant.now());
    }

    public ChangeableClock(Instant instant) {
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

    public void forward(long millis) {
        instant = instant.plusMillis(millis);
    }

    public void add(long delay, TimeUnit timeUnit) {
        instant = instant.plusMillis(timeUnit.toMillis(delay));
    }
}
