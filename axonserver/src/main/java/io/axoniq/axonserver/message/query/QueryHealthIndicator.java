/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.refactoring.transport.grpc.QueryService;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class QueryHealthIndicator extends AbstractHealthIndicator {

    private final QueryService queryService;

    public QueryHealthIndicator(QueryService queryService) {
        this.queryService = queryService;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        builder.up();
        queryService.listeners().forEach(listener-> {
            builder.withDetail(String.format("%s.waitingQueries", listener.queue()), listener.waiting());
            builder.withDetail(String.format("%s.permits", listener.queue()), listener.permits());
        });

    }
}
