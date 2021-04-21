/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.transport.grpc;

import io.axoniq.axonserver.ClientStreamIdentification;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ExceptionUtils;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandServiceGrpc;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.refactoring.configuration.TopologyEvents.CommandHandlerDisconnected;
import io.axoniq.axonserver.refactoring.configuration.topology.Topology;
import io.axoniq.axonserver.refactoring.messaging.api.Client;
import io.axoniq.axonserver.refactoring.messaging.api.Registration;
import io.axoniq.axonserver.refactoring.messaging.command.CommandDispatcher;
import io.axoniq.axonserver.refactoring.messaging.command.LegacyCommandHandler;
import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommand;
import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommandProviderInbound;
import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommandResponse;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandDefinition;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;
import io.axoniq.axonserver.refactoring.requestprocessor.command.CommandService;
import io.axoniq.axonserver.refactoring.transport.ClientIdRegistry;
import io.axoniq.axonserver.refactoring.transport.ContextProvider;
import io.axoniq.axonserver.refactoring.transport.Mapper;
import io.axoniq.axonserver.refactoring.transport.heartbeat.ApplicationInactivityException;
import io.axoniq.axonserver.refactoring.transport.instruction.InstructionAckSource;
import io.axoniq.axonserver.refactoring.transport.rest.SpringAuthentication;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;

import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

/**
 * GRPC service to handle command bus requests from Axon Application
 * Client can sent two requests:
 * dispatch: sends a singe command to AxonServer
 * openStream: used by application providing command handlers, maintains an open bi directional connection between the
 * application and AxonServer
 *
 * @author Marc Gathier
 */
@Service("CommandGrpcService")
public class CommandGrpcService implements AxonServerClientService {

    private static final MethodDescriptor<byte[], SerializedCommandResponse> METHOD_DISPATCH =
            CommandServiceGrpc.getDispatchMethod().toBuilder(ByteArrayMarshaller.instance(),
                                                             ProtoUtils.marshaller(SerializedCommandResponse
                                                                                           .getDefaultInstance()))
                              .build();

    private static final MethodDescriptor<CommandProviderOutbound, SerializedCommandProviderInbound> METHOD_OPEN_STREAM =
            CommandServiceGrpc.getOpenStreamMethod().toBuilder(ProtoUtils.marshaller(CommandProviderOutbound
                                                                                             .getDefaultInstance()),
                                                               ProtoUtils.marshaller(SerializedCommandProviderInbound
                                                                                             .getDefaultInstance()))
                              .build();

    private final CommandService commandService;

    private final Topology topology;
    private final CommandDispatcher commandDispatcher;
    private final ContextProvider contextProvider;
    private final AuthenticationProvider authenticationProvider;
    private final ClientIdRegistry clientIdRegistry;
    private final ApplicationEventPublisher eventPublisher;
    private final Logger logger = LoggerFactory.getLogger(CommandGrpcService.class);
    private final Map<ClientStreamIdentification, GrpcFlowControlledDispatcherListener> dispatcherListeners = new ConcurrentHashMap<>();
    private final InstructionAckSource<SerializedCommandProviderInbound> instructionAckSource;
    private final Mapper<CommandResponse, SerializedCommandResponse> commandResponseMapper;
    private final Mapper<Command, SerializedCommand> commandMapper;

    @Value("${axoniq.axonserver.command-threads:1}")
    private int processingThreads = 1;

    public CommandGrpcService(CommandService commandService,
                              Topology topology,
                              CommandDispatcher commandDispatcher,
                              ContextProvider contextProvider,
                              AuthenticationProvider authenticationProvider,
                              ClientIdRegistry clientIdRegistry,
                              ApplicationEventPublisher eventPublisher,
                              @Qualifier("commandInstructionAckSource")
                                      InstructionAckSource<SerializedCommandProviderInbound> instructionAckSource,
                              @Qualifier("commandResponseMapper")
                                      Mapper<CommandResponse, SerializedCommandResponse> commandResponseMapper,
                              @Qualifier("commandMapper")
                              Mapper<Command, SerializedCommand> commandMapper) {
        this.commandService = commandService;
        this.topology = topology;
        this.commandDispatcher = commandDispatcher;
        this.contextProvider = contextProvider;
        this.authenticationProvider = authenticationProvider;
        this.clientIdRegistry = clientIdRegistry;
        this.eventPublisher = eventPublisher;
        this.instructionAckSource = instructionAckSource;
        this.commandResponseMapper = commandResponseMapper;
        this.commandMapper = commandMapper;
    }

    @PreDestroy
    public void cleanup() {
        dispatcherListeners.forEach((client, listener) -> listener.cancel());
        dispatcherListeners.clear();
    }

    public Set<GrpcFlowControlledDispatcherListener> listeners() {
        return new HashSet<>(dispatcherListeners.values());
    }

    @Override
    public final ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(CommandServiceGrpc.SERVICE_NAME)
                                      .addMethod(
                                              METHOD_OPEN_STREAM,
                                              asyncBidiStreamingCall(this::openStream))
                                      .addMethod(
                                              METHOD_DISPATCH,
                                              asyncUnaryCall(this::dispatch))
                                      .build();
    }


    public StreamObserver<CommandProviderOutbound> openStream(
            StreamObserver<SerializedCommandProviderInbound> responseObserver) {
        SpringAuthentication springAuthentication = new SpringAuthentication(authenticationProvider.get());
        String context = contextProvider.getContext();
        SendingStreamObserver<SerializedCommandProviderInbound> wrappedResponseObserver = new SendingStreamObserver<>(
                responseObserver);
        return new ReceivingStreamObserver<CommandProviderOutbound>(logger) {
            private final Map<String, MonoSink<CommandResponse>> commandsInFlight = new ConcurrentHashMap<>();
            private final Map<String, Registration> registrations = new ConcurrentHashMap<>();
            private final AtomicReference<String> clientIdRef = new AtomicReference<>();
            private final AtomicReference<ClientStreamIdentification> clientRef = new AtomicReference<>();
            private final AtomicReference<GrpcCommandDispatcherListener> listenerRef = new AtomicReference<>();
            private final AtomicReference<LegacyCommandHandler<?>> commandHandlerRef = new AtomicReference<>();

            @Override
            protected void consume(CommandProviderOutbound commandFromSubscriber) {
                switch (commandFromSubscriber.getRequestCase()) {
                    case SUBSCRIBE:
                        instructionAckSource.sendSuccessfulAck(commandFromSubscriber.getInstructionId(),
                                                               wrappedResponseObserver);
                        CommandSubscription subscribe = commandFromSubscriber.getSubscribe();

                        checkInitClient(subscribe.getClientId(), subscribe.getComponentName());
                        commandService.register(new io.axoniq.axonserver.refactoring.messaging.command.api.CommandHandler() {
                            @Override
                            public CommandDefinition definition() {
                                return new CommandDefinition() {
                                    @Override
                                    public String name() {
                                        return subscribe.getCommand();
                                    }

                                    @Override
                                    public String context() {
                                        return context;
                                    }
                                };
                            }

                            @Override
                            public Client client() {
                                return new Client() {
                                    @Override
                                    public String id() {
                                        return subscribe.getClientId();
                                    }

                                    @Override
                                    public String applicationName() {
                                        return subscribe.getComponentName();
                                    }
                                };
                            }

                            @Override
                            public Mono<CommandResponse> handle(Command command) {
                                return Mono.create(sink -> {
                                    commandsInFlight.put(command.message().id(), sink);
                                    SerializedCommandProviderInbound request = SerializedCommandProviderInbound
                                            .newBuilder()
                                            .setCommand(commandMapper.map(command))
                                            .build();

                                    responseObserver.onNext(request);
                                });
                            }
                        }, springAuthentication).subscribe(r -> registrations.put(subscribe.getCommand(), r));
                        break;
                    case UNSUBSCRIBE:
                        instructionAckSource.sendSuccessfulAck(commandFromSubscriber.getInstructionId(),
                                                               wrappedResponseObserver);
                        Registration registration = registrations.remove(commandFromSubscriber.getUnsubscribe()
                                                                                              .getCommand());
                        if (registration != null) {
                            registration.cancel();
                        }
                        break;
                    case FLOW_CONTROL:
                        instructionAckSource.sendSuccessfulAck(commandFromSubscriber.getInstructionId(),
                                                               wrappedResponseObserver);
                        flowControl(commandFromSubscriber.getFlowControl());
                        break;
                    case COMMAND_RESPONSE:
                        instructionAckSource.sendSuccessfulAck(commandFromSubscriber.getInstructionId(),
                                                               wrappedResponseObserver);
                        // TODO: 4/20/2021 consider edge cases
                        commandsInFlight.get(commandFromSubscriber.getCommandResponse().getRequestIdentifier())
                                        .success(commandResponseMapper.unmap(new SerializedCommandResponse(commandFromSubscriber
                                                                                                                   .getCommandResponse())));
                        break;
                    case ACK:
                        InstructionAck ack = commandFromSubscriber.getAck();
                        if (isUnsupportedInstructionErrorResult(ack)) {
                            logger.warn("Unsupported command instruction sent to the client of context {}.", context);
                        } else {
                            logger.trace("Received command instruction ack {}.", ack);
                        }
                        break;
                    default:
                        instructionAckSource.sendUnsupportedInstruction(commandFromSubscriber.getInstructionId(),
                                                                        topology.getMe().getName(),
                                                                        wrappedResponseObserver);
                        break;
                }
            }

            private void initClientReference(String clientId) {
                String clientStreamId = clientId + "." + UUID.randomUUID().toString();
                if (clientRef.compareAndSet(null, new ClientStreamIdentification(context, clientStreamId))) {
                    clientIdRegistry.register(clientStreamId, clientId, ClientIdRegistry.ConnectionType.COMMAND);
                }
                clientIdRef.compareAndSet(null, clientId);
            }

            private void flowControl(FlowControl flowControl) {
                initClientReference(flowControl.getClientId());
                if (listenerRef.compareAndSet(null,
                                              new GrpcCommandDispatcherListener(commandDispatcher.getCommandQueues(),
                                                                                clientRef.get().toString(),
                                                                                wrappedResponseObserver,
                                                                                processingThreads))) {
                    dispatcherListeners.put(clientRef.get(), listenerRef.get());
                }
                listenerRef.get().addPermits(flowControl.getPermits());
            }

            private void checkInitClient(String clientId, String component) {
                initClientReference(clientId);
//                commandHandlerRef.compareAndSet(null,
//                                                new DirectCommandHandler(wrappedResponseObserver,
//                                                                         clientRef.get(),
//                                                                         clientId,
//                                                                         component));
            }

            @Override
            protected String sender() {
                return clientRef.toString();
            }

            @Override
            public void onError(Throwable cause) {
                if (!ExceptionUtils.isCancelled(cause)) {
                    logger.warn("{}: Error on connection from subscriber - {}", clientRef, cause.getMessage());
                }
                cleanup();
            }

            private void cleanup() {
                registrations.forEach((command, registration) -> registration.cancel());
                GrpcCommandDispatcherListener listener = listenerRef.get();
                if (listener != null) {
                    listener.cancel();
                    dispatcherListeners.remove(clientRef.get());
                }
                StreamObserverUtils.complete(responseObserver);
            }

            @Override
            public void onCompleted() {
                logger.debug("{}: Connection to subscriber closed by subscriber", clientRef);
                cleanup();
            }
        };
    }

    private boolean isUnsupportedInstructionErrorResult(InstructionAck instructionResult) {
        return instructionResult.hasError()
                && instructionResult.getError().getErrorCode().equals(ErrorCode.UNSUPPORTED_INSTRUCTION.getCode());
    }

    public void dispatch(byte[] command, StreamObserver<SerializedCommandResponse> responseObserver) {
        SerializedCommand request = new SerializedCommand(command, contextProvider.getContext());
        String clientId = request.wrapped().getClientId();


        commandService.execute(commandMapper.unmap(request),
                               new SpringAuthentication(authenticationProvider.get()))
                      .map(commandResponseMapper::map)
                      .doOnError(error -> logger.warn("Dispatching failed with unexpected error", error))
                      .subscribe(response -> safeReply(clientId, response, responseObserver),
                                 error -> responseObserver.onError(GrpcExceptionBuilder.build(error))
                      );
    }

    private void safeReply(String clientId, SerializedCommandResponse commandResponse,
                           StreamObserver<SerializedCommandResponse> responseObserver) {
        try {
            responseObserver.onNext(commandResponse);
            responseObserver.onCompleted();
        } catch (RuntimeException ex) {
            logger.debug("Response to client {} failed", clientId, ex);
        }
    }

    /**
     * Completes the command stream to the specified client.
     *
     * @param clientId                   the unique identifier of the client instance
     * @param clientStreamIdentification the unique identifier of the command stream
     */
    public void completeStreamForInactivity(String clientId, ClientStreamIdentification clientStreamIdentification) {
        if (dispatcherListeners.containsKey(clientStreamIdentification)) {
            String message = "Command stream inactivity for " + clientStreamIdentification.getClientStreamId();
            ApplicationInactivityException exception = new ApplicationInactivityException(message);
            dispatcherListeners.remove(clientStreamIdentification).cancelAndCompleteStreamExceptionally(exception);
            logger.debug("Command Stream closed for client: {}", clientStreamIdentification);
            eventPublisher.publishEvent(new CommandHandlerDisconnected(clientStreamIdentification.getContext(),
                                                                       clientId,
                                                                       clientStreamIdentification.getClientStreamId()));
        }
    }
}
