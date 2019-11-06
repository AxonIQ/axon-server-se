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

import java.util.function.BiFunction;

/**
 * @author Marc Gathier
 */
public interface MetricCollector extends BiFunction<String, Tags, ClusterMetric> {

    Iterable<AxonServerMetric> getAll(String metricName, Tags tags);
}
