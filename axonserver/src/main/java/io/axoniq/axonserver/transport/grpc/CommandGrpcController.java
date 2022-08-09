/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.commandprocesing.imp.CommandDispatcher;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.component.tags.ClientTagsCache;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.command.CommandServiceGrpc;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.transport.grpc.command.GrpcCommand;
import io.axoniq.axonserver.transport.grpc.command.GrpcCommandResult;
import io.axoniq.axonserver.transport.grpc.command.GrpcMapper;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class CommandGrpcController extends CommandServiceGrpc.CommandServiceImplBase
        implements AxonServerClientService {

    private final Logger logger = LoggerFactory.getLogger(CommandGrpcController.class);
    private final ContextProvider contextProvider;
    private final AuthenticationProvider authenticationProvider;
    private final CommandRequestProcessor commandRequestProcessor;

    private final CommandDispatcher queuedCommandDispatcher;
    private final ClientTagsCache clientTagsCache;


    private final Map<UUID, CommandHandlerStream> commandHandlers = new ConcurrentHashMap<>();

    public CommandGrpcController(ContextProvider contextProvider, AuthenticationProvider authenticationProvider,
                                 CommandRequestProcessor commandRequestProcessor,
                                 CommandDispatcher queuedCommandDispatcher,
                                 ClientTagsCache clientTagsCache) {
        this.contextProvider = contextProvider;
        this.authenticationProvider = authenticationProvider;
        this.commandRequestProcessor = commandRequestProcessor;
        this.queuedCommandDispatcher = queuedCommandDispatcher;
        this.clientTagsCache = clientTagsCache;
    }

    @Override
    public StreamObserver<CommandProviderOutbound> openStream(StreamObserver<CommandProviderInbound> responseObserver) {
        return new StreamObserver<CommandProviderOutbound>() {
            private final UUID id = UUID.randomUUID();

            @Override
            public void onNext(CommandProviderOutbound commandProviderOutbound) {
                CommandHandlerStream commandHandlerStream =
                        commandHandlers.computeIfAbsent(id, id -> new CommandHandlerStream(contextProvider.getContext(),
                                                                                           clientId(
                                                                                                   commandProviderOutbound),
                                                                                           responseObserver));

                switch (commandProviderOutbound.getRequestCase()) {
                    case SUBSCRIBE:
                        commandHandlerStream.subscribe(commandProviderOutbound.getSubscribe())
                                            .subscribe(ignore -> {
                                                       },
                                                       Throwable::printStackTrace,
                                                       () -> sendAck(commandProviderOutbound.getInstructionId()));
                        break;
                    case UNSUBSCRIBE:
                        commandHandlerStream.unsubscribe(commandProviderOutbound.getUnsubscribe())
                                            .subscribe(ignore -> {
                                                       },
                                                       Throwable::printStackTrace,
                                                       () -> sendAck(commandProviderOutbound.getInstructionId()));
                        break;
                    case FLOW_CONTROL:
                        commandHandlerStream.flowControl(commandProviderOutbound.getFlowControl());
                        break;
                    case COMMAND_RESPONSE:
                        commandHandlerStream.commandResponse(commandProviderOutbound.getCommandResponse());
                        break;
                    case ACK:
                        // ignore
                        break;
                    case REQUEST_NOT_SET:
                        break;
                }
            }

            private void sendAck(String instructionId) {
                responseObserver.onNext(CommandProviderInbound.newBuilder()
                                                              .setAck(InstructionAck.newBuilder()
                                                                                    .setInstructionId(instructionId)
                                                                                    .build())
                                                              .build());
            }

            private String clientId(CommandProviderOutbound request) {
                switch (request.getRequestCase()) {
                    case SUBSCRIBE:
                        return request.getSubscribe().getClientId();
                    case UNSUBSCRIBE:
                        return request.getUnsubscribe().getClientId();
                    case FLOW_CONTROL:
                        return request.getFlowControl().getClientId();
                    default:
                        return "Unknown handler id";
                }
            }

            @Override
            public void onError(Throwable throwable) {
                onCompleted();
            }

            @Override
            public void onCompleted() {
                CommandHandlerStream stream = commandHandlers.remove(id);
                if (stream != null) {
                    stream.cancel();
                }
            }
        };
    }

    @Override
    public void dispatch(Command request, StreamObserver<CommandResponse> responseObserver) {
        String context = contextProvider.getContext();
        Authentication authentication = authenticationProvider.get();
        commandRequestProcessor.dispatch(new GrpcCommand(request,
                        context,
                        new GrpcAuthentication(
                                () -> authentication)))
                .subscribe(
                        result -> responseObserver.onNext(GrpcMapper.map(result)),
                        error -> returnError(responseObserver, request, error),
                        responseObserver::onCompleted);
    }

    private void returnError(StreamObserver<CommandResponse> responseObserver, Command request, Throwable e) {
        CommandResponse commandResponse = CommandResponse.newBuilder()
                                                         .setRequestIdentifier(request.getMessageIdentifier())
                                                         .setMessageIdentifier(UUID.randomUUID().toString())
                                                         .setErrorMessage(ErrorMessage.newBuilder()
                                                                                      .setMessage(e.toString()))
                                                         .build();

        responseObserver.onNext(commandResponse);
        responseObserver.onCompleted();
    }

    private class CommandHandlerStream {

        private final String context;
        private final String clientId;
        private final StreamObserver<CommandProviderInbound> streamToHandler;

        private final Map<String, String> registrations = new ConcurrentHashMap<>();
        private final Map<String, MonoSink<CommandResponse>> activeCommands = new ConcurrentHashMap<>();

        public CommandHandlerStream(String context, String clientId,
                                    StreamObserver<CommandProviderInbound> streamToHandler) {
            this.context = context;
            this.clientId = clientId;
            this.streamToHandler = streamToHandler;
        }

        public Mono<Void> subscribe(CommandSubscription subscribe) {
            GrpcCommandHandlerSubscription handler = new GrpcCommandHandlerSubscription(subscribe,
                                                                                        clientId,
                                                                                        context,
                                                                                        () -> clientTagsCache.get(
                                                                                                clientId,
                                                                                                context),
                                                                                        this::dispatch
            );
            return commandRequestProcessor.register(handler)
                                          .doOnSuccess(registration -> registrations.put(subscribe.getCommand(),
                                                                                         handler.commandHandler.id()))
                                          .then();
        }

        public Mono<Void> unsubscribe(CommandSubscription unsubscribe) {
            String handlerId = registrations.remove(unsubscribe.getCommand());
            if (handlerId != null) {
                return commandRequestProcessor.unregister(handlerId);
            }
            return Mono.empty();
        }

        public void flowControl(FlowControl flowControl) {
            queuedCommandDispatcher.request(clientId, flowControl.getPermits());
        }

        public void commandResponse(CommandResponse commandResponse) {
            MonoSink<CommandResponse> sink = activeCommands.remove(commandResponse.getRequestIdentifier());
            if (sink != null) {
                sink.success(commandResponse);
            }
        }

        public void cancel() {
            registrations.forEach((unused, handlerId) -> commandRequestProcessor.unregister(handlerId).block());
            // cancelling active commands should be done on unsubscribe in the command request processor
            activeCommands.forEach((requestId, sink) -> sink.error(new MessagingPlatformException(ErrorCode.CONNECTION_TO_HANDLER_LOST,
                                                                                                  clientId
                                                                                                          + ": connection lost")));
        }

        private Mono<CommandResponse> dispatch(Command command) {
            return Mono.create(sink -> {
                activeCommands.put(command.getMessageIdentifier(), sink);
                streamToHandler.onNext(CommandProviderInbound.newBuilder()
                                                             .setCommand(command)
                                                             .build());
            });
        }
    }

    private class GrpcCommandHandlerSubscription implements CommandHandlerSubscription {

        private final CommandHandler commandHandler;
        private final Function<Command, Mono<CommandResponse>> dispatchOperation;
        private final String clientId;

        public GrpcCommandHandlerSubscription(CommandSubscription subscribe,
                                              String clientId,
                                              String context,
                                              Supplier<Map<String, String>> clientTagsProvider,
                                              Function<Command, Mono<CommandResponse>> dispatchOperation) {
            this.clientId = clientId;
            this.commandHandler = new CommandHandler() {
                private final String id = UUID.randomUUID().toString();

                @Override
                public String id() {
                    return id;
                }

                @Override
                public String description() {
                    return subscribe.getCommand() + " at " + subscribe.getClientId();
                }

                @Override
                public String commandName() {
                    return subscribe.getCommand();
                }

                @Override
                public String context() {
                    return context;
                }

                @Override
                public Metadata metadata() {
                    Map<String, Serializable> clientMetadata = new HashMap<>(clientTagsProvider.get());
                    clientMetadata.put(LOAD_FACTOR, subscribe.getLoadFactor());
                    clientMetadata.put(CLIENT_ID, subscribe.getClientId());
                    clientMetadata.put(COMPONENT_NAME, subscribe.getComponentName());
                    return new Metadata() {
                        @Override
                        public Iterable<String> metadataKeys() {
                            return clientMetadata.keySet();
                        }

                        @Override
                        public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                            return Optional.ofNullable((R) clientMetadata.get(metadataKey));
                        }
                    };
                }
            };
            this.dispatchOperation = dispatchOperation;
        }

        @Override
        public CommandHandler commandHandler() {
            return commandHandler;
        }

        @Override
        public Mono<CommandResult> dispatch(io.axoniq.axonserver.commandprocessing.spi.Command command) {
            return dispatchOperation.apply(GrpcMapper.map(command)).map(result -> map(result, clientId));
        }
    }

    private CommandResult map(CommandResponse commandResponse, String clientId) {
        CommandResponse response = CommandResponse.newBuilder(commandResponse)
                                                  .putMetaData(CommandResult.CLIENT_ID,
                                                               MetaDataValue.newBuilder().setTextValue(clientId)
                                                                            .build())
                                                  .build();
        return new GrpcCommandResult(response);
    }
}
