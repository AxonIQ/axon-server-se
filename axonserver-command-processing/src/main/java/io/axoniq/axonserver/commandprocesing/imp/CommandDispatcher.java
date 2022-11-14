/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerUnsubscribedInterceptor;
import io.axoniq.axonserver.commons.health.HealthMonitoredComponent;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;

public interface CommandDispatcher extends CommandHandlerUnsubscribedInterceptor, HealthMonitoredComponent {

    Mono<CommandResult> dispatch(CommandHandlerSubscription handler, Command commandRequest);

    default void request(String clientId, long count) {}

    @Override
    default Health health() {
        return new Health() {
            @Override
            public Status status() {
                return Status.UP;
            }

            @Override
            public Map<String, String> details() {
                return Collections.emptyMap();
            }
        };
    }

    @Override
    default String healthCategory() {
        return "command";
    }
}
