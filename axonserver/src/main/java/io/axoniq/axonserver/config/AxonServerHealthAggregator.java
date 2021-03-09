/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import org.springframework.boot.actuate.health.SimpleStatusAggregator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

/**
 * Aggregator to determine the overall health of the server. Adds logic for setting status to WARN if one of the
 * parts is in WARN status.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Component
@Primary
public class AxonServerHealthAggregator extends SimpleStatusAggregator {

    public AxonServerHealthAggregator() {
        super(Status.DOWN, Status.OUT_OF_SERVICE, HealthStatus.WARN_STATUS,
              Status.UP, Status.UNKNOWN);
    }
}
