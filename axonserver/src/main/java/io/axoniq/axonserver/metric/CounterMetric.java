/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

import java.util.function.Supplier;

/**
 * Created by Sara Pellegrini on 22/06/2018.
 * sara.pellegrini@gmail.com
 */
public class CounterMetric implements ClusterMetric {

    private final String name;
    private final Supplier<Long> valueProvider;

    public CounterMetric(String name, Supplier<Long> valueProvider) {
        this.name = name;
        this.valueProvider = valueProvider;
    }

    public String getName() {
        return name;
    }

    @Override
    public long count() {
        return valueProvider.get();
    }

    @Override
    public double min() {
        return 0;
    }

    @Override
    public double max() {
        return 0;
    }

    @Override
    public double mean() {
        return 0;
    }

    @Override
    public double value() {
        return 0;
    }
}
