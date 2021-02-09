/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ErrorMessageFactory;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.interceptor.CommandInterceptors;
import io.axoniq.axonserver.interceptor.DefaultInterceptorContext;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.util.ConstraintCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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
    private final ConstraintCache<String, CommandInformation> commandCache;
    private final CommandMetricsRegistry metricRegistry;
    private final Logger logger = LoggerFactory.getLogger(CommandDispatcher.class);
    private final FlowControlQueues<WrappedCommand> commandQueues;
    private final Map<String, MeterFactory.RateMeter> commandRatePerContext = new ConcurrentHashMap<>();
    private final CommandInterceptors commandInterceptors;

    public CommandDispatcher(CommandRegistrationCache registrations,
                             ConstraintCache<String, CommandInformation> commandCache,
                             CommandMetricsRegistry metricRegistry,
                             MeterFactory meterFactory,
                             CommandInterceptors commandInterceptors,
                             @Value("${axoniq.axonserver.command-queue-capacity-per-client:10000}") int queueCapacity) {
        this.registrations = registrations;
        this.commandCache = commandCache;
        this.metricRegistry = metricRegistry;
        this.commandInterceptors = commandInterceptors;
        commandQueues = new FlowControlQueues<>(Comparator.comparing(WrappedCommand::priority).reversed(),
                                                queueCapacity,
                                                BaseMetricName.AXON_APPLICATION_COMMAND_QUEUE_SIZE,
                                                meterFactory,
                                                ErrorCode.COMMAND_DISPATCH_ERROR);
        metricRegistry.gauge(BaseMetricName.AXON_ACTIVE_COMMANDS, commandCache, ConstraintCache::size);
    }


    public void dispatchProxied(String context, SerializedCommand request,
                                Consumer<SerializedCommandResponse> responseObserver) {
        String clientStreamId = request.getClientStreamId();
        ClientStreamIdentification clientIdentification = new ClientStreamIdentification(context, clientStreamId);
        CommandHandler<?> handler = registrations.findByClientAndCommand(clientIdentification,
                                                                         request.getCommand());
        dispatchToCommandHandler(request, handler, responseObserver,
                                 ErrorCode.CLIENT_DISCONNECTED,
                                 String.format("Client %s not found while processing: %s"
                                         , clientStreamId, request.getCommand()));
    }

    public void dispatch(String context, Authentication authentication, SerializedCommand request,
                         Consumer<SerializedCommandResponse> responseObserver) {
        ExtensionUnitOfWork extensionUnitOfWork = new DefaultInterceptorContext(context, authentication);
        Consumer<SerializedCommandResponse> interceptedResponseObserver = r -> intercept(extensionUnitOfWork,
                                                                                         r,
                                                                                         responseObserver);
        try {
            request = commandInterceptors.commandRequest(request, extensionUnitOfWork);
            commandRate(context).mark();
            CommandHandler<?> commandHandler = registrations.getHandlerForCommand(context,
                                                                                  request.wrapped(),
                                                                                  request.getRoutingKey());
            dispatchToCommandHandler(request,
                                     commandHandler,
                                     interceptedResponseObserver,
                                     ErrorCode.NO_HANDLER_FOR_COMMAND,
                                     "No Handler for command: " + request.getCommand()
            );
        } catch (MessagingPlatformException other) {
            logger.warn("{}: Exception dispatching command {}", context, request.getCommand(), other);
            interceptedResponseObserver.accept(errorCommandResponse(request.getMessageIdentifier(),
                                                                    other.getErrorCode(),
                                                                    other.getMessage()));
        } catch (Exception other) {
            logger.warn("{}: Exception dispatching command {}", context, request.getCommand(), other);
            interceptedResponseObserver.accept(errorCommandResponse(request.getMessageIdentifier(),
                                                                    ErrorCode.OTHER,
                                                                    other.getMessage()));
        }
    }

    private void intercept(ExtensionUnitOfWork extensionUnitOfWork,
                           SerializedCommandResponse response,
                           Consumer<SerializedCommandResponse> responseObserver) {
        try {
            responseObserver.accept(commandInterceptors.commandResponse(response, extensionUnitOfWork));
        } catch (MessagingPlatformException ex) {
            logger.warn("{}: Exception in response interceptor", extensionUnitOfWork.context(), ex);
            responseObserver.accept(errorCommandResponse(response.getRequestIdentifier(),
                                                         ex.getErrorCode(),
                                                         ex.getMessage()));
        } catch (Exception other) {
            logger.warn("{}: Exception in response interceptor", extensionUnitOfWork.context(), other);
            responseObserver.accept(errorCommandResponse(response.getRequestIdentifier(),
                                                         ErrorCode.OTHER,
                                                         other.getMessage()));
        }
    }


    public MeterFactory.RateMeter commandRate(String context) {
        return commandRatePerContext.computeIfAbsent(context,
                                                     c -> metricRegistry
                                                             .rateMeter(c, BaseMetricName.AXON_COMMAND_RATE));
    }

    @EventListener
    public void on(TopologyEvents.CommandHandlerDisconnected event) {
        handleDisconnection(event.clientIdentification(), event.isProxied());
    }

    private void handleDisconnection(ClientStreamIdentification client, boolean proxied) {
        cleanupRegistrations(client);
        if (!proxied) {
            getCommandQueues().move(client.toString(), this::redispatch);
        }
        handlePendingCommands(client);
    }

    private void dispatchToCommandHandler(SerializedCommand command, CommandHandler<?> commandHandler,
                                          Consumer<SerializedCommandResponse> responseObserver,
                                          ErrorCode noHandlerErrorCode, String noHandlerMessage) {
        if (commandHandler == null) {
            logger.warn("No Handler for command: {}", command.getName());
            responseObserver.accept(errorCommandResponse(command.getMessageIdentifier(),
                                                         noHandlerErrorCode,
                                                         noHandlerMessage));
            return;
        }

        try {
            logger.debug("Dispatch {} to: {}", command.getName(), commandHandler.getClientStreamIdentification());
            CommandInformation commandInformation = new CommandInformation(command.getName(),
                                                                           command.wrapped().getClientId(),
                                                                           commandHandler.getClientId(),
                                                                           responseObserver,
                                                                           commandHandler
                                                                                   .getClientStreamIdentification(),
                                                                           commandHandler.getComponentName());
            commandCache.put(command.getMessageIdentifier(), commandInformation);
            WrappedCommand wrappedCommand = new WrappedCommand(commandHandler.getClientStreamIdentification(),
                                                               commandHandler.getClientId(),
                                                               command);
            commandQueues.put(commandHandler.queueName(), wrappedCommand, wrappedCommand.priority());
        } catch (InsufficientBufferCapacityException insufficientBufferCapacityException) {
            responseObserver.accept(errorCommandResponse(command.getMessageIdentifier(),
                                                         ErrorCode.TOO_MANY_REQUESTS,
                                                         insufficientBufferCapacityException
                                                                 .getMessage()));
        } catch (MessagingPlatformException mpe) {
            commandCache.remove(command.getMessageIdentifier());
            responseObserver.accept(errorCommandResponse(command.getMessageIdentifier(),
                                                         mpe.getErrorCode(),
                                                         mpe.getMessage()));
        }
    }



    public void handleResponse(SerializedCommandResponse commandResponse, boolean proxied) {
        CommandInformation toPublisher = commandCache.remove(commandResponse.getRequestIdentifier());
        if (toPublisher != null) {
            logger.debug("Sending response to: {}", toPublisher);
            if (!proxied) {
                metricRegistry.add(toPublisher.getRequestIdentifier(),
                                   toPublisher.getSourceClientId(),
                                   toPublisher.getTargetClientId(),
                                   toPublisher.getClientStreamIdentification().getContext(),
                                   System.currentTimeMillis() - toPublisher.getTimestamp());
            }
            toPublisher.getResponseConsumer().accept(commandResponse);
        } else {
            logger.info("Could not find command request: {}", commandResponse.getRequestIdentifier());
        }
    }

    private void cleanupRegistrations(ClientStreamIdentification client) {
        registrations.remove(client);
    }

    public FlowControlQueues<WrappedCommand> getCommandQueues() {
        return commandQueues;
    }

    private String redispatch(WrappedCommand command) {
        SerializedCommand request = command.command();
        CommandInformation commandInformation = commandCache.remove(request.getMessageIdentifier());
        if (commandInformation == null) {
            return null;
        }

        CommandHandler<?> client = registrations.getHandlerForCommand(command.client().getContext(), request.wrapped(),
                                                                      request.getRoutingKey());
        if (client == null) {
            commandInformation.getResponseConsumer().accept(errorCommandResponse(request.getMessageIdentifier(),
                                                                                 ErrorCode.NO_HANDLER_FOR_COMMAND,
                                                                                 "No Handler for command: "
                                                                                         + request
                                                                                         .getName()));
            return null;
        }

        logger.debug("Dispatch {} to: {}", request.getName(), client.getClientStreamIdentification());
        commandCache.put(request.getMessageIdentifier(), new CommandInformation(request.getName(),
                                                                                request.wrapped().getClientId(),
                                                                                client.getClientId(),
                                                                                commandInformation
                                                                                        .getResponseConsumer(),
                                                                                client.getClientStreamIdentification(),
                                                                                client.getComponentName()));
        return client.queueName();
    }

    private void handlePendingCommands(ClientStreamIdentification client) {
        List<String> messageIds = commandCache.entrySet().stream().filter(e -> e.getValue().checkClient(client))
                                              .map(Map.Entry::getKey).collect(Collectors.toList());

        messageIds.forEach(m -> {
            CommandInformation ci = commandCache.remove(m);
            if (ci != null) {
                ci.getResponseConsumer()
                  .accept(errorCommandResponse(m,
                                               ErrorCode.CONNECTION_TO_HANDLER_LOST,
                                               "Connection lost while executing command on: " + ci
                                                       .getTargetClientId()));
            }
        });
    }

    public int activeCommandCount() {
        return commandCache.size();
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
