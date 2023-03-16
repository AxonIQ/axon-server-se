/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.util.DaemonThreadFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

@Component
public class MaintenanceExecutors implements Supplier<ScheduledExecutorService> {

    private ScheduledExecutorService maintenanceScheduler = Executors.newScheduledThreadPool(2,
                                                                                             new DaemonThreadFactory(
                                                                                                     "maintenance-scheduler"));

    @Override
    public ScheduledExecutorService get() {
        return maintenanceScheduler;
    }
}
