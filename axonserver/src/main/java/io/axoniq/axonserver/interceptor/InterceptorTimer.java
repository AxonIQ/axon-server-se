/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author Marc Gathier
 */
public class InterceptorTimer {

    private final MeterFactory meterFactory;

    public InterceptorTimer(MeterFactory meterFactory) {
        this.meterFactory = meterFactory;
    }

    public <R> R time(String context, String interceptor, Supplier<R> action) {
        long before = System.currentTimeMillis();
        try {
            return action.get();
        } finally {
            record(context, interceptor, System.currentTimeMillis() - before);
        }
    }

    public void time(String context, String interceptor, Runnable action) {
        long before = System.currentTimeMillis();
        try {
            action.run();
        } finally {
            record(context, interceptor, System.currentTimeMillis() - before);
        }
    }

    private void record(String context, String interceptor, long millis) {
        Timer timer = meterFactory.timer(BaseMetricName.INTERCEPTOR_DURATION,
                                         Tags.of(MeterFactory.CONTEXT,
                                                 context,
                                                 "interceptor",
                                                 interceptor));
        timer.record(millis, TimeUnit.MILLISECONDS);
    }
}
