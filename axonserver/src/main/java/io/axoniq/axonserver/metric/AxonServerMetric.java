/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

/**
 * Base definition of a  metric used for monitoring durations.
 * @author Marc Gathier
 * @since 4.0
 */
public interface AxonServerMetric {

    double getValue();

    double getMin();

    double getMax();

    double getMean();

    String getName();

    long getCount();
}
