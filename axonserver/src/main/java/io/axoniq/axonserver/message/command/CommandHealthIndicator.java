/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.config.HealthStatus;
import io.axoniq.axonserver.grpc.CommandService;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class CommandHealthIndicator extends AbstractHealthIndicator {

    private final CommandService commandService;

    public CommandHealthIndicator(CommandService commandService) {
        this.commandService = commandService;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        builder.up();
        commandService.listeners().forEach(listener-> {
            if (listener.waiting() > 0 && listener.permits() <= 0) {
                builder.status(HealthStatus.WARN_STATUS);
            }
            builder.withDetail(String.format("%s.waitingCommands", listener.queue()), listener.waiting());
            builder.withDetail(String.format("%s.permits", listener.queue()), listener.permits());
        });

    }
}
