/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

import java.util.Collections;

/**
 * @author Marc Gathier
 */
public class DefaultMetricCollector implements MetricCollector {

    @Override
    public Iterable<AxonServerMetric> getAll() {
        return Collections.emptyList();
    }

    @Override
    public ClusterMetric apply(String s) {
        return new CounterMetric(s, () -> 0L);
    }
}
