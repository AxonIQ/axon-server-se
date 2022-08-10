/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.axonserver.config.HealthStatus.WARN_STATUS;

/**
 * @author Marc Gathier
 */
@Component
public class CommandHealthIndicator extends AbstractHealthIndicator {

    private final MeterRegistry meterRegistry;

    public CommandHealthIndicator(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        AtomicInteger waiting = new AtomicInteger();
        meterRegistry.find("commands.queued").meters().forEach(meter -> {
            String queue = meter.getId().getTag("QUEUE");
            int queued = (int) ((Gauge) meter).value();
            builder.withDetail(String.format("%s.waitingCommands", queue), queued);
            waiting.addAndGet(queued);
        });
        if (waiting.get() > 10) {
            builder.status(WARN_STATUS);
        } else {
            builder.up();
        }
    }
}
