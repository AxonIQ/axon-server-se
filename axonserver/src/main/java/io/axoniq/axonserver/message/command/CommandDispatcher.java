/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ErrorMessageFactory;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.interceptor.CommandInterceptors;
import io.axoniq.axonserver.interceptor.DefaultExecutionContext;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

/**
 * Responsible for managing command subscriptions and processing commands.
 * Subscriptions are stored in the {@link CommandRegistrationCache}.
 * Running commands are stored in the {@link CommandCache}.
 *
 * @author Marc Gathier
 */
@Component("CommandDispatcher")
public class CommandDispatcher {

    private final CommandRegistrationCache registrations;
    private final CommandMetricsRegistry metricRegistry;
    private final Logger logger = LoggerFactory.getLogger(CommandDispatcher.class);
    private final Map<String, MeterFactory.RateMeter> commandRatePerContext = new ConcurrentHashMap<>();
    private final CommandInterceptors commandInterceptors;
    private final CapacityValidator capacityValidator;
    private final Duration commandTimeout;
    private final AtomicInteger activeCommands = new AtomicInteger();

    public CommandDispatcher(CommandRegistrationCache registrations,
                             CommandMetricsRegistry metricRegistry,
                             CommandInterceptors commandInterceptors,
                             CapacityValidator capacityValidator,
                             @Value("${axoniq.axonserver.default-command-timeout:300000}") long defaultCommandTimeout) {
        this.registrations = registrations;
        this.metricRegistry = metricRegistry;
        this.commandInterceptors = commandInterceptors;
        this.capacityValidator = capacityValidator;
        this.commandTimeout = Duration.ofMillis(defaultCommandTimeout);
        metricRegistry.gauge(BaseMetricName.AXON_ACTIVE_COMMANDS, activeCommands, AtomicInteger::get);
    }


    public Mono<SerializedCommandResponse> dispatchProxied(String context, SerializedCommand request) {
        String clientStreamId = request.getClientStreamId();
        ClientStreamIdentification clientIdentification = new ClientStreamIdentification(context, clientStreamId);
        CommandHandler handler = registrations.findByClientAndCommand(clientIdentification,
                                                                         request.getCommand());
        return dispatchToCommandHandler(request, handler,
                                 context,
                                 ErrorCode.CLIENT_DISCONNECTED,
                                 String.format("Client %s not found while processing: %s"
                                         , clientStreamId, request.getCommand()));
    }

    public Mono<SerializedCommandResponse> dispatch(String context, Authentication authentication, SerializedCommand request) {
        DefaultExecutionContext executionContext = new DefaultExecutionContext(context, authentication);
        long time = System.currentTimeMillis();
        try {
            SerializedCommand interceptedRequest = commandInterceptors.commandRequest(request, executionContext);
            commandRate(context).mark();
            CommandHandler commandHandler = registrations.getHandlerForCommand(context,
                                                                               interceptedRequest.wrapped(),
                                                                               interceptedRequest.getRoutingKey());
            return dispatchToCommandHandler(interceptedRequest,
                                     commandHandler,
                                     context,
                                     ErrorCode.NO_HANDLER_FOR_COMMAND,
                                     "No Handler for command: " + request.getCommand()
            ).map(r -> intercept(executionContext, r))
                    .doOnSuccess(response -> metricRegistry.add(request.getCommand(),
                                                            request.wrapped().getClientId(),
                                                            commandHandler == null ? "NOT_FOUND" : commandHandler.getClientId(),
                                                            context,
                                                            System.currentTimeMillis() - time)
            );
        } catch (MessagingPlatformException other) {
            logger.warn("{}: Exception dispatching command {}", context, request.getCommand(), other);
            executionContext.compensate(other);
            return Mono.just(errorCommandResponse(request.getMessageIdentifier(),
                                                                    other.getErrorCode(),
                                                                    other.getMessage()));
        } catch (Exception other) {
            logger.warn("{}: Exception dispatching command {}", context, request.getCommand(), other);
            executionContext.compensate(other);
            return Mono.just(errorCommandResponse(request.getMessageIdentifier(),
                                                                    ErrorCode.OTHER,
                                                                    other.getMessage()));
        }
    }

    private SerializedCommandResponse intercept(DefaultExecutionContext executionContext,
                           SerializedCommandResponse response) {
        try {
            return commandInterceptors.commandResponse(response, executionContext);
        } catch (MessagingPlatformException ex) {
            logger.warn("{}: Exception in response interceptor", executionContext.contextName(), ex);
            executionContext.compensate(ex);
            return errorCommandResponse(response.getRequestIdentifier(),
                                                         ex.getErrorCode(),
                                                         ex.getMessage());
        } catch (Exception other) {
            logger.warn("{}: Exception in response interceptor", executionContext.contextName(), other);
            executionContext.compensate(other);
            return errorCommandResponse(response.getRequestIdentifier(),
                                                         ErrorCode.OTHER,
                                                         other.getMessage());
        }
    }


    public MeterFactory.RateMeter commandRate(String context) {
        return commandRatePerContext.computeIfAbsent(context,
                                                     c -> metricRegistry
                                                             .rateMeter(c, BaseMetricName.AXON_COMMAND_RATE));
    }

    private Mono<SerializedCommandResponse> dispatchToCommandHandler(SerializedCommand command,
                                                                     CommandHandler commandHandler,
                                                                     String context,
                                                                     ErrorCode noHandlerErrorCode, String noHandlerMessage) {
        if (commandHandler == null) {
            logger.warn("No Handler for command: {}", command.getName());
            return Mono.just(errorCommandResponse(command.getMessageIdentifier(),
                                                         noHandlerErrorCode,
                                                         noHandlerMessage));
        }

        try {
            Ticket ticket = capacityValidator.ticket(context);
            logger.debug("Dispatch {} to: {}", command.getName(), commandHandler.getClientStreamIdentification());
            activeCommands.incrementAndGet();
            return commandHandler.dispatch(command)
                                 .timeout(commandTimeout)
                                 .doOnTerminate(() -> {
                                     ticket.release();
                                     activeCommands.decrementAndGet();
                                 });
        } catch (InsufficientBufferCapacityException insufficientBufferCapacityException) {
            return Mono.just(errorCommandResponse(command.getMessageIdentifier(),
                                                         ErrorCode.TOO_MANY_REQUESTS,
                                                         insufficientBufferCapacityException
                                                                 .getMessage()));
        } catch (MessagingPlatformException mpe) {
            return Mono.just(errorCommandResponse(command.getMessageIdentifier(),
                                                         mpe.getErrorCode(),
                                                         mpe.getMessage()));
        }
    }



    public int activeCommandCount() {
        return activeCommands.get();
    }

    @Nonnull
    private SerializedCommandResponse errorCommandResponse(String requestIdentifier, ErrorCode errorCode,
                                                           String errorMessage) {
        return new SerializedCommandResponse(CommandResponse.newBuilder()
                                                            .setMessageIdentifier(UUID.randomUUID().toString())
                                                            .setRequestIdentifier(requestIdentifier)
                                                            .setErrorCode(errorCode
                                                                                  .getCode())
                                                            .setErrorMessage(ErrorMessageFactory
                                                                                     .build(errorMessage))
                                                            .build());
    }
}
