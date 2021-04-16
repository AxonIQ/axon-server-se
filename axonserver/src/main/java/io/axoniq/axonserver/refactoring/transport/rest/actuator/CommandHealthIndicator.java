/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.transport.rest.actuator;

import io.axoniq.axonserver.refactoring.transport.grpc.CommandGrpcService;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class CommandHealthIndicator extends AbstractHealthIndicator {

    private final CommandGrpcService commandService;

    public CommandHealthIndicator(CommandGrpcService commandService) {
        this.commandService = commandService;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        builder.up();
        commandService.listeners().forEach(listener -> {
            builder.withDetail(String.format("%s.waitingCommands", listener.queue()), listener.waiting());
            builder.withDetail(String.format("%s.permits", listener.queue()), listener.permits());
        });
    }
}
