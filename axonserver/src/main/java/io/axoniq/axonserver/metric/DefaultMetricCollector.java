/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

import io.micrometer.core.instrument.Tags;

import java.util.Collections;

/**
 * @author Marc Gathier
 */
public class DefaultMetricCollector implements MetricCollector {

    @Override
    public Iterable<AxonServerMetric> getAll(String name, Tags tags) {
        return Collections.emptyList();
    }

    @Override
    public ClusterMetric apply(String name, Tags tags) {
        return new CounterMetric(name, () -> 0L);
    }
}
