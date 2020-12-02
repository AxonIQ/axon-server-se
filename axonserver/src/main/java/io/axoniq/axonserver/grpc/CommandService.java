/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeCommand;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents.UnsubscribeCommand;
import io.axoniq.axonserver.applicationevents.TopologyEvents.CommandHandlerDisconnected;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ExceptionUtils;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandServiceGrpc;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.grpc.heartbeat.ApplicationInactivityException;
import io.axoniq.axonserver.message.ByteArrayMarshaller;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.DirectCommandHandler;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.StreamObserverUtils;
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
@Service("CommandService")
public class CommandService implements AxonServerClientService {

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


    private final Topology topology;
    private final CommandDispatcher commandDispatcher;
    private final ContextProvider contextProvider;
    private final AuthenticationProvider authenticationProvider;
    private final ClientIdRegistry clientIdRegistry;
    private final ApplicationEventPublisher eventPublisher;
    private final Logger logger = LoggerFactory.getLogger(CommandService.class);
    private final Map<ClientStreamIdentification, GrpcFlowControlledDispatcherListener> dispatcherListeners = new ConcurrentHashMap<>();
    private final InstructionAckSource<SerializedCommandProviderInbound> instructionAckSource;

    @Value("${axoniq.axonserver.command-threads:1}")
    private int processingThreads = 1;

    public CommandService(Topology topology,
                          CommandDispatcher commandDispatcher,
                          ContextProvider contextProvider,
                          AuthenticationProvider authenticationProvider,
                          ClientIdRegistry clientIdRegistry,
                          ApplicationEventPublisher eventPublisher,
                          @Qualifier("commandInstructionAckSource")
                                  InstructionAckSource<SerializedCommandProviderInbound> instructionAckSource) {
        this.topology = topology;
        this.commandDispatcher = commandDispatcher;
        this.contextProvider = contextProvider;
        this.authenticationProvider = authenticationProvider;
        this.clientIdRegistry = clientIdRegistry;
        this.eventPublisher = eventPublisher;
        this.instructionAckSource = instructionAckSource;
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
        String context = contextProvider.getContext();
        SendingStreamObserver<SerializedCommandProviderInbound> wrappedResponseObserver = new SendingStreamObserver<>(
                responseObserver);
        return new ReceivingStreamObserver<CommandProviderOutbound>(logger) {
            private final AtomicReference<String> clientIdRef = new AtomicReference<>();
            private final AtomicReference<ClientStreamIdentification> clientRef = new AtomicReference<>();
            private final AtomicReference<GrpcCommandDispatcherListener> listenerRef = new AtomicReference<>();
            private final AtomicReference<CommandHandler<?>> commandHandlerRef = new AtomicReference<>();

            @Override
            protected void consume(CommandProviderOutbound commandFromSubscriber) {
                switch (commandFromSubscriber.getRequestCase()) {
                    case SUBSCRIBE:
                        instructionAckSource.sendSuccessfulAck(commandFromSubscriber.getInstructionId(),
                                                               wrappedResponseObserver);
                        CommandSubscription subscribe = commandFromSubscriber.getSubscribe();

                        checkInitClient(subscribe.getClientId(), subscribe.getComponentName());
                        SubscribeCommand event = new SubscribeCommand(context,
                                                                      clientRef.get().getClientStreamId(),
                                                                      subscribe,
                                                                      commandHandlerRef.get());
                        eventPublisher.publishEvent(event);
                        break;
                    case UNSUBSCRIBE:
                        instructionAckSource.sendSuccessfulAck(commandFromSubscriber.getInstructionId(),
                                                               wrappedResponseObserver);
                        if (clientRef.get() != null) {
                            UnsubscribeCommand unsubscribe =
                                    new UnsubscribeCommand(context,
                                                           clientRef.get().getClientStreamId(),
                                                           commandFromSubscriber.getUnsubscribe(),
                                                           false);
                            eventPublisher.publishEvent(unsubscribe);
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
                        commandDispatcher.handleResponse(new SerializedCommandResponse(commandFromSubscriber
                                                                                               .getCommandResponse()),
                                                         false);
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
                commandHandlerRef.compareAndSet(null,
                                                new DirectCommandHandler(wrappedResponseObserver,
                                                                         clientRef.get(),
                                                                         clientId,
                                                                         component));
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
                if (clientRef.get() != null) {
                    String clientStreamId = clientRef.get().getClientStreamId();
                    String clientId = clientIdRef.get();
                    clientIdRegistry.unregister(clientStreamId, ClientIdRegistry.ConnectionType.COMMAND);
                    eventPublisher.publishEvent(new CommandHandlerDisconnected(clientRef.get().getContext(),
                                                                               clientId,
                                                                               clientStreamId));
                }
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
        SerializedCommand request = new SerializedCommand(command);
        String clientId = request.wrapped().getClientId();
        if (logger.isTraceEnabled()) {
            logger.trace("{}: Received command: {}", clientId, request.wrapped().getName());
        }
        try {
            commandDispatcher.dispatch(contextProvider.getContext(),
                                       authenticationProvider.get(),
                                       request,
                                       commandResponse -> safeReply(clientId,
                                                                    commandResponse,
                                                                    responseObserver));
        } catch (Exception ex) {
            logger.warn("Dispatching failed with unexpected error", ex);
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
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
