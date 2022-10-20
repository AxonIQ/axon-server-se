/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandException;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandFailedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandReceivedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandResultReceivedInterceptor;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class CommandsPerContextCounter {

    private final Map<String, AtomicInteger> activeCommandsCounters = new ConcurrentHashMap<>();
    private final Map<String, String> commandContext = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;

    public CommandsPerContextCounter(CommandRequestProcessor commandRequestProcessor, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        commandRequestProcessor.registerInterceptor(CommandReceivedInterceptor.class, this::receivedCommand);
        commandRequestProcessor.registerInterceptor(CommandResultReceivedInterceptor.class, this::receivedResponse);
        commandRequestProcessor.registerInterceptor(CommandFailedInterceptor.class, this::commandFailed);
    }

    private Mono<CommandException> commandFailed(Mono<CommandException> commandExceptionMono) {
        return commandExceptionMono.doOnNext(r -> decrementCounter(r.command().id()));
    }

    private void decrementCounter(String id) {
        Optional.ofNullable(commandContext.remove(id))
                .flatMap(context -> Optional.ofNullable(activeCommandsCounters.get(context)))
                .ifPresent(AtomicInteger::decrementAndGet);
    }

    private Mono<CommandResult> receivedResponse(Mono<CommandResult> commandResultMono) {
        return commandResultMono.doOnNext(r -> decrementCounter(r.commandId()));
    }

    private Mono<Command> receivedCommand(Mono<Command> commandMono) {
        return commandMono.flatMap(command -> {
            commandContext.put(command.id(), command.context());
            activeCommandsCounters.computeIfAbsent(command.context(), c -> {
                AtomicInteger counter = new AtomicInteger();
                Gauge.builder("axon.commands.active", () -> counter)
                     .tags(MeterFactory.CONTEXT, c)
                     .register(meterRegistry);
                return counter;
            }).incrementAndGet();
            return Mono.just(command);
        });
    }

    public long get(String context) {
        return Optional.ofNullable(activeCommandsCounters.get(context))
                       .map(AtomicInteger::get)
                       .orElse(0);
    }
}
