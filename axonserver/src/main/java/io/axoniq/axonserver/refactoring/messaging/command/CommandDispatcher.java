/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.messaging.command;

import io.axoniq.axonserver.ClientStreamIdentification;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.refactoring.api.Authentication;
import io.axoniq.axonserver.refactoring.configuration.TopologyEvents;
import io.axoniq.axonserver.refactoring.messaging.ConstraintCache;
import io.axoniq.axonserver.refactoring.messaging.FlowControlQueues;
import io.axoniq.axonserver.refactoring.messaging.InsufficientBufferCapacityException;
import io.axoniq.axonserver.refactoring.messaging.MessagingPlatformException;
import io.axoniq.axonserver.refactoring.messaging.api.Error;
import io.axoniq.axonserver.refactoring.messaging.api.Message;
import io.axoniq.axonserver.refactoring.messaging.api.Registration;
import io.axoniq.axonserver.refactoring.messaging.api.SerializedObject;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandHandler;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandHandlerRegistry;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandRouter;
import io.axoniq.axonserver.refactoring.metric.BaseMetricName;
import io.axoniq.axonserver.refactoring.metric.MeterFactory;
import io.axoniq.axonserver.refactoring.plugin.DefaultExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
public class CommandDispatcher implements CommandRouter, CommandHandlerRegistry {

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


//    public void dispatchProxied(String context, SerializedCommand request,
//                                Consumer<SerializedCommandResponse> responseObserver) {
//        String clientStreamId = request.getClientStreamId();
//        ClientStreamIdentification clientIdentification = new ClientStreamIdentification(context, clientStreamId);
//        CommandHandler<?> handler = registrations.findByClientAndCommand(clientIdentification,
//                                                                         request.getCommand());
//        dispatchToCommandHandler(request, handler, responseObserver,
//                                 ErrorCode.CLIENT_DISCONNECTED,
//                                 String.format("Client %s not found while processing: %s"
//                                         , clientStreamId, request.getCommand()));
//    }

    @Deprecated
    public Mono<CommandResponse> dispatchCommand(Command command, Authentication authentication) {
        String context = command.context();
        String name = command.definition().name();
        DefaultExecutionContext executionContext = new DefaultExecutionContext(context, authentication);

        try {
            command = commandInterceptors.commandRequest(command, executionContext);
            commandRate(context).mark();
            CommandHandler commandHandler = registrations.getHandlerForCommand(command,
                                                                               command.routingKey());
            return dispatchToCommandHandler(command,
                                            commandHandler,
                                            ErrorCode.NO_HANDLER_FOR_COMMAND,
                                            "No Handler for command: " + name
            ).map(response -> intercept(executionContext, response));
        } catch (MessagingPlatformException other) {
            logger.warn("{}: Exception dispatching command {}", context, name, other);
            executionContext.compensate(other);
            return Mono.just(errorCommandResponse(command.message().id(),
                                                  other.getErrorCode(),
                                                  other.getMessage()));
        } catch (Exception other) {
            logger.warn("{}: Exception dispatching command {}", context, name, other);
            executionContext.compensate(other);
            return Mono.just(errorCommandResponse(command.message().id(),
                                                  ErrorCode.OTHER,
                                                  other.getMessage()));
        }
    }

    private CommandResponse intercept(DefaultExecutionContext executionContext,
                                      CommandResponse response) {
        try {
            return commandInterceptors.commandResponse(response, executionContext);
        } catch (MessagingPlatformException ex) {
            logger.warn("{}: Exception in response interceptor", executionContext.contextName(), ex);
            executionContext.compensate(ex);
            return errorCommandResponse(response.requestId(),
                                        ex.getErrorCode(),
                                        ex.getMessage());
        } catch (Exception other) {
            logger.warn("{}: Exception in response interceptor", executionContext.contextName(), other);
            executionContext.compensate(other);
            return errorCommandResponse(response.requestId(),
                                        ErrorCode.OTHER,
                                        other.getMessage());
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
//        if (!proxied) {
//            getCommandQueues().move(client.toString(), this::redispatch);
//        }
        handlePendingCommands(client);
    }

    private Mono<CommandResponse> dispatchToCommandHandler(Command command, CommandHandler commandHandler,
                                                           ErrorCode noHandlerErrorCode, String noHandlerMessage) {
        if (commandHandler == null) {
            logger.warn("No Handler for command: {}", command.definition().name());
            Mono.just(errorCommandResponse(command.message().id(),
                                           noHandlerErrorCode,
                                           noHandlerMessage));
        }

        try {
            logger.debug("Dispatch {} to: {}", command.definition().name(), commandHandler.client().id());
            CommandInformation commandInformation = new CommandInformation(command.definition().name(),
                                                                           command.requester().id(),
                                                                           commandHandler.client().id(),
                                                                           null,
                                                                           null,
                                                                           commandHandler.client().applicationName());
            commandCache.put(command.message().id(), commandInformation);
//            WrappedCommand wrappedCommand = new WrappedCommand(commandHandler.getClientStreamIdentification(),
//                                                               commandHandler.getClientId(),
//                                                               command);
            return commandHandler.handle(command);
        } catch (InsufficientBufferCapacityException insufficientBufferCapacityException) {
            return Mono.just(errorCommandResponse(command.message().id(),
                                                  ErrorCode.TOO_MANY_REQUESTS,
                                                  insufficientBufferCapacityException
                                                          .getMessage()));
        } catch (MessagingPlatformException mpe) {
            commandCache.remove(command.message().id());
            return Mono.just(errorCommandResponse(command.message().id(),
                                                  mpe.getErrorCode(),
                                                  mpe.getMessage()));
        }
    }


//    public void handleResponse(SerializedCommandResponse commandResponse, boolean proxied) {
//        CommandInformation toPublisher = commandCache.remove(commandResponse.getRequestIdentifier());
//        if (toPublisher != null) {
//            logger.debug("Sending response to: {}", toPublisher);
//            if (!proxied) {
//                metricRegistry.add(toPublisher.getRequestIdentifier(),
//                                   toPublisher.getSourceClientId(),
//                                   toPublisher.getTargetClientId(),
//                                   toPublisher.getClientStreamIdentification().getContext(),
//                                   System.currentTimeMillis() - toPublisher.getTimestamp());
//            }
//            toPublisher.getResponseConsumer().accept(commandResponse);
//        } else {
//            logger.info("Could not find command request: {}", commandResponse.getRequestIdentifier());
//        }
//    }

    private void cleanupRegistrations(ClientStreamIdentification client) {
        registrations.remove(client);
    }

    public FlowControlQueues<WrappedCommand> getCommandQueues() {
        return commandQueues;
    }

//    private String redispatch(WrappedCommand command) {
//        SerializedCommand request = command.command();
//        CommandInformation commandInformation = commandCache.remove(request.getMessageIdentifier());
//        if (commandInformation == null) {
//            return null;
//        }
//
//        CommandHandler<?> client = registrations.getHandlerForCommand(command.client().getContext(), request.wrapped(),
//                                                                      request.getRoutingKey());
//        if (client == null) {
//            commandInformation.getResponseConsumer().accept(errorCommandResponse(request.getMessageIdentifier(),
//                                                                                 ErrorCode.NO_HANDLER_FOR_COMMAND,
//                                                                                 "No Handler for command: "
//                                                                                         + request
//                                                                                         .getName()));
//            return null;
//        }
//
//        logger.debug("Dispatch {} to: {}", request.getName(), client.getClientStreamIdentification());
//        commandCache.put(request.getMessageIdentifier(), new CommandInformation(request.getName(),
//                                                                                request.wrapped().getClientId(),
//                                                                                client.getClientId(),
//                                                                                commandInformation
//                                                                                        .getResponseConsumer(),
//                                                                                client.getClientStreamIdentification(),
//                                                                                client.getComponentName()));
//        return client.queueName();
//    }

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
    private CommandResponse errorCommandResponse(String requestIdentifier, ErrorCode errorCode,
                                                 String errorMessage) {
        return new io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse() {

            @Override
            public String requestId() {
                return requestIdentifier;
            }

            @Override
            public Message message() {
                return new Message() {
                    @Override
                    public String id() {
                        return UUID.randomUUID().toString();
                    }

                    @Override
                    public Optional<SerializedObject> payload() {
                        return Optional.empty();
                    }

                    @Override
                    public <T> T metadata(String key) {
                        return null;
                    }

                    @Override
                    public Set<String> metadataKeys() {
                        return Collections.emptySet();
                    }
                };
            }

            @Override
            public Optional<Error> error() {
                return Optional.of(new Error() {
                    @Override
                    public String code() {
                        return errorCode.getCode();
                    }

                    @Override
                    public String message() {
                        return errorMessage;
                    }

                    @Override
                    public List<String> details() {
                        return Collections.emptyList();
                    }

                    @Override
                    public String source() {
                        return "AxonServer";
                    }
                });
            }
        };
    }

    @Override
    public Mono<CommandResponse> dispatch(
            Authentication authentication, Command command) {
        return dispatchCommand(command,
                               authentication).doOnTerminate(() -> commandCache.remove(command.message().id()));
    }

    @Override
    public Mono<Registration> register(
            io.axoniq.axonserver.refactoring.messaging.command.api.CommandHandler commandHandler) {
        return Mono.create(sink -> {
            registrations.add(commandHandler);
            sink.success(() -> {
                ClientStreamIdentification clientStreamIdentification =
                        new ClientStreamIdentification(commandHandler.context(), commandHandler.client().id());
                registrations.remove(clientStreamIdentification, commandHandler.definition().name());
            });
        });
    }

    @Override
    public List<io.axoniq.axonserver.refactoring.messaging.command.api.CommandHandler> handlers() {
        // TODO: 4/20/2021 implement :)
        return new ArrayList<>();
    }

//    class MappingCommandResponse implements io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse {
//
//        private final SerializedCommandResponse serializedCommandResponse;
//
//        MappingCommandResponse(
//                SerializedCommandResponse serializedCommandResponse) {
//            this.serializedCommandResponse = serializedCommandResponse;
//        }
//
//        @Override
//        public String requestId() {
//            return serializedCommandResponse.getRequestIdentifier();
//        }
//
//        @Override
//        public Message message() {
//            return new Message() {
//                @Override
//                public String id() {
//                    return serializedCommandResponse.wrapped().getMessageIdentifier();
//                }
//
//                @Override
//                public Optional<SerializedObject> payload() {
//                    if (!serializedCommandResponse.wrapped().hasPayload()) {
//                        return Optional.empty();
//                    }
//                    io.axoniq.axonserver.grpc.SerializedObject payload = serializedCommandResponse.wrapped().getPayload();
//                    return Optional.of(new SerializedObject() {
//                        @Override
//                        public String type() {
//                            return payload.getType();
//                        }
//
//                        @Override
//                        public String revision() {
//                            return payload.getRevision();
//                        }
//
//                        @Override
//                        public byte[] data() {
//                            return payload.getData().toByteArray();
//                        }
//                    });
//                }
//
//                @Override
//                public <T> T metadata(String key) {
//                    //TODO
//                    return (T) serializedCommandResponse.wrapped().getMetaDataMap().get(key);
//                }
//
//                @Override
//                public Set<String> metadataKeys() {
//                    return serializedCommandResponse.wrapped().getMetaDataMap().keySet();
//                }
//            };
//        }
//
//        @Override
//        public Optional<Error> error() {
//            if (serializedCommandResponse.getErrorCode().isEmpty()) {
//                return Optional.empty();
//            }
//
//            return Optional.of(new Error() {
//
//                @Override
//                public String code() {
//                    return serializedCommandResponse.getErrorCode();
//                }
//
//                @Override
//                public String message() {
//                    return serializedCommandResponse.wrapped().getErrorMessage().getMessage();
//                }
//
//                @Override
//                public List<String> details() {
//                    return serializedCommandResponse.wrapped().getErrorMessage().getDetailsList();
//                }
//
//                @Override
//                public String source() {
//                    return serializedCommandResponse.wrapped().getErrorMessage().getLocation();
//                }
//            });
//        }
//    }
}
