/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationConnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationInactivityTimeout;
import io.axoniq.axonserver.component.tags.ClientTagsUpdate;
import io.axoniq.axonserver.component.version.ClientVersionUpdate;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ExceptionUtils;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.NodeInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.RequestCase;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformServiceGrpc;
import io.axoniq.axonserver.grpc.control.RequestReconnect;
import io.axoniq.axonserver.grpc.heartbeat.ApplicationInactivityException;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.topology.AxonServerNode;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * gRPC service to track connected applications. Each application will first call the openStream operation with a
 * register request to retrieve information on which Axon Server node to connect to (Standard edition will always return
 * current node as node to connect to).
 *
 * @author Marc Gathier
 * @since 4.0
 */
@Service("PlatformService")
public class PlatformService extends PlatformServiceGrpc.PlatformServiceImplBase implements AxonServerClientService {

    private static final Logger logger = LoggerFactory.getLogger(PlatformService.class);

    private final Map<ClientComponent, SendingStreamObserver<PlatformOutboundInstruction>> connectionMap = new ConcurrentHashMap<>();
    private final Topology topology;
    private final ContextProvider contextProvider;
    private final ApplicationEventPublisher eventPublisher;
    private final Map<RequestCase, Deque<InstructionConsumer>> handlers = new EnumMap<>(RequestCase.class);
    private final InstructionAckSource<PlatformOutboundInstruction> instructionAckSource;
    private final ClientIdRegistry clientIdRegistry;

    /**
     * Instantiate a {@link PlatformService}, used to track all connected applications and deal with internal events.
     *
     * @param topology             the {@link Topology} of the group this Axon Server instance participates in
     * @param contextProvider      a {@link ContextProvider} used to retrieve the context this Axon Server instance is
     *                             working under
     * @param clientIdRegistry     registry to keep track of connected control streams
     * @param eventPublisher       the {@link ApplicationEventPublisher} to publish events through this Axon Server
     * @param instructionAckSource responsible for sending instruction acknowledgements
     */
    public PlatformService(Topology topology,
                           ContextProvider contextProvider,
                           ClientIdRegistry clientIdRegistry,
                           ApplicationEventPublisher eventPublisher,
                           @Qualifier("platformInstructionAckSource")
                                   InstructionAckSource<PlatformOutboundInstruction> instructionAckSource) {
        this.topology = topology;
        this.contextProvider = contextProvider;
        this.clientIdRegistry = clientIdRegistry;
        this.eventPublisher = eventPublisher;
        this.instructionAckSource = instructionAckSource;
        onInboundInstruction(RequestCase.ACK, (client, instruction) -> {
            InstructionAck ack = instruction.getAck();
            if (isUnsupportedInstructionErrorResult(ack)) {
                logger.warn("Unsupported instruction sent to the client {} of context {}.",
                            client.clientStreamId,
                            client.context);
            } else {
                logger.trace("Received instruction ack from the client {} of context {}. Result {}.",
                             client.clientStreamId,
                             client.context,
                             ack);
            }
        });
    }

    private boolean isUnsupportedInstructionErrorResult(InstructionAck instructionAck) {
        return instructionAck.hasError()
                && instructionAck.getError().getErrorCode().equals(ErrorCode.UNSUPPORTED_INSTRUCTION.getCode());
    }

    @Override
    public void getPlatformServer(ClientIdentification request, StreamObserver<PlatformInfo> responseObserver) {
        String context = contextProvider.getContext();
        try {
            AxonServerNode connectTo = topology.findNodeForClient(request.getClientId(),
                                                                  request.getComponentName(),
                                                                  context,
                                                                  request.getTagsMap());
            responseObserver.onNext(PlatformInfo.newBuilder()
                                                .setSameConnection(connectTo.getName().equals(topology.getName()))
                                                .setPrimary(NodeInfo.newBuilder().setNodeName(connectTo.getName())
                                                                    .setHostName(connectTo.getHostName())
                                                                    .setGrpcPort(connectTo.getGrpcPort())
                                                                    .setHttpPort(connectTo.getHttpPort())
                                                ).build());
            responseObserver.onCompleted();
        } catch (MessagingPlatformException cause) {
            logger.info("Error finding target for client {}/{}: {}", request.getClientId(),
                        context,
                        cause.getMessage());
            responseObserver.onError(GrpcExceptionBuilder.build(cause));
        } catch (RuntimeException cause) {
            logger.warn("Error processing client request {}", request, cause);
            responseObserver.onError(GrpcExceptionBuilder.build(cause));
        }
    }

    @Override
    public StreamObserver<PlatformInboundInstruction> openStream(
            StreamObserver<PlatformOutboundInstruction> responseObserver) {
        String context = contextProvider.getContext();
        SendingStreamObserver<PlatformOutboundInstruction> sendingStreamObserver =
                new SendingStreamObserver<>(responseObserver);

        return new ReceivingStreamObserver<PlatformInboundInstruction>(logger) {
            private final AtomicReference<ClientComponent> clientComponent = new AtomicReference<>();

            @Override
            protected void consume(PlatformInboundInstruction instruction) {
                RequestCase requestCase = instruction.getRequestCase();
                if (instruction.hasRegister()) { // TODO: 11/1/2019 register this as instruction handler
                    instructionAckSource.sendSuccessfulAck(instruction.getInstructionId(), sendingStreamObserver);
                    ClientIdentification client = instruction.getRegister();
                    String clientId = client.getClientId();
                    String clientStreamId = clientId + "." + UUID.randomUUID();
                    clientIdRegistry.register(clientStreamId,
                                              new ClientContext(clientId, context),
                                              ClientIdRegistry.ConnectionType.PLATFORM);
                    eventPublisher.publishEvent(new ClientTagsUpdate(clientStreamId,
                                                                     context,
                                                                     client.getTagsMap()));

                    ClientComponent clientComponent = new ClientComponent(clientStreamId,
                                                                          clientId,
                                                                          client.getComponentName(),
                                                                          context);
                    this.clientComponent.compareAndSet(null, clientComponent);
                    registerClient(this.clientComponent.get(), sendingStreamObserver);
                    eventPublisher.publishEvent(new ClientVersionUpdate(clientStreamId,
                                                                        context,
                                                                        client.getVersion()));
                } else if (!handlers.containsKey(requestCase)) {
                    instructionAckSource.sendUnsupportedInstruction(instruction.getInstructionId(),
                                                                    topology.getMe().getName(),
                                                                    sendingStreamObserver);
                } else {
                    instructionAckSource.sendSuccessfulAck(instruction.getInstructionId(),
                                                           sendingStreamObserver);
                    if (clientComponent.get() != null) {
                        handlers.getOrDefault(requestCase, new ArrayDeque<>())
                                .forEach(consumer -> consumer.accept(this.clientComponent.get(),
                                                                     instruction));
                    }
                }
            }

            @Override
            protected String sender() {
                return clientComponent.get() == null ? null : clientComponent.get().clientStreamId;
            }

            @Override
            public void onError(Throwable throwable) {
                if (!ExceptionUtils.isCancelled(throwable)) {
                    logger.warn("{}: error on connection - {}", sender(), throwable.getMessage());
                }
                deregisterClient(clientComponent.get(), "Error on platform connection from client");
            }

            @Override
            public void onCompleted() {
                deregisterClient(clientComponent.get(), "Platform connection completed by client");
            }
        };
    }

    public boolean requestReconnect(ClientComponent clientName, String reason) {
        logger.debug("Request reconnect: {} - {}", clientName, reason);

        StreamObserver<PlatformOutboundInstruction> stream = connectionMap.get(clientName);
        if (stream != null) {
            eventPublisher.publishEvent(new TopologyEvents.ApplicationReconnectRequested(clientName.context,
                                                                                         clientName.clientId,
                                                                                         clientName.component,
                                                                                         reason));
            stream.onNext(PlatformOutboundInstruction.newBuilder()
                                                     .setRequestReconnect(RequestReconnect.newBuilder())
                                                     .build());
            return true;
        }
        return false;
    }

    public boolean requestReconnect(String clientId, String reason) {
        logger.debug("Request reconnect: {}", clientId);
        return connectionMap.keySet()
                            .stream()
                            .filter(e -> e.clientId.equals(clientId))
                            .map(e -> requestReconnect(e, reason))
                            .findFirst().orElse(false);
    }

    /**
     * Sends the specified instruction to all the clients that are directly connected to this instance of AxonServer.
     *
     * @param clientStreamId the client id for platform stream
     * @param instruction    the {@link PlatformInboundInstruction} to be sent
     */
    public void sendToClientStreamId(String clientStreamId, PlatformOutboundInstruction instruction) {
        List<SendingStreamObserver<PlatformOutboundInstruction>> stream =
                connectionMap.entrySet().stream()
                             .filter(e -> clientStreamId.equals(e.getKey().clientStreamId))
                             .map(Map.Entry::getValue)
                             .collect(Collectors.toList());
        stream.forEach(s -> s.onNext(instruction));
    }


    /**
     * Sends the specified instruction to all the clients that are directly connected to this instance of AxonServer.
     *
     * @param context     the context of the connected client
     * @param clientId    the unique identifier of the client
     * @param instruction the {@link PlatformInboundInstruction} to be sent
     */
    public void sendToClient(String context, String clientId, PlatformOutboundInstruction instruction) {
        connectionMap.entrySet().stream()
                     .filter(e -> e.getKey().clientId.equals(clientId))
                     .filter(e -> e.getKey().context.equals(context))
                     .map(Map.Entry::getValue)
                     .forEach(connection -> connection.onNext(instruction));
    }

    @EventListener
    public void on(ApplicationDisconnected event) {
        StreamObserver<PlatformOutboundInstruction> connection = connectionMap
                .remove(new ClientComponent(event.getClientStreamId(),
                                            event.getClientId(),
                                            event.getComponentName(),
                                            event.getContext()));
        logger.debug("application disconnected: {}, connection: {}", event.getClientStreamId(), connection);
        if (connection != null) {
            try {
                connection.onCompleted();
            } catch (Exception ex) {
                logger.debug("Error while closing tracking event processor connection from {} - {}",
                             event.getClientStreamId(),
                             ex.getMessage());
            }
        }
    }

    public void onInboundInstruction(RequestCase requestCase, InstructionConsumer consumer) {
        Deque<InstructionConsumer> consumers = handlers.computeIfAbsent(requestCase, rc -> new ArrayDeque<>());
        consumers.add(consumer);
    }


    private void registerClient(ClientComponent clientComponent,
                                SendingStreamObserver<PlatformOutboundInstruction> responseObserver) {
        logger.debug("Registered client : {}", clientComponent);

        connectionMap.put(clientComponent, responseObserver);
        eventPublisher.publishEvent(new ApplicationConnected(clientComponent.context,
                                                             clientComponent.component,
                                                             clientComponent.clientStreamId,
                                                             clientComponent.clientId,
                                                             null
        ));
    }

    private void deregisterClient(ClientComponent clientComponent, String reason) {
        logger.debug("De-registered client : {}", clientComponent);

        if (clientComponent != null) {
            SendingStreamObserver<PlatformOutboundInstruction> stream = connectionMap.remove(clientComponent);
            if (stream != null) {
                StreamObserverUtils.complete(stream);
            }
            clientIdRegistry.unregister(clientComponent.clientStreamId, ClientIdRegistry.ConnectionType.PLATFORM);

            eventPublisher.publishEvent(new ApplicationDisconnected(
                    clientComponent.context,
                    clientComponent.component,
                    clientComponent.clientStreamId,
                    clientComponent.clientId,
                    null,
                    reason
            ));
        }
    }

    private void deregisterClient(ClientComponent clientComponent, Throwable cause) {
        SendingStreamObserver<PlatformOutboundInstruction> stream = connectionMap.remove(clientComponent);
        if (stream != null) {
            StreamObserverUtils.error(stream, cause);
        }

        deregisterClient(clientComponent, cause.getMessage());
    }

    /**
     * De-registers a client if it turns out to be inactive/not properly connected
     *
     * @param evt the event of inactivity timeout for a specific client component
     */
    @EventListener
    public void on(ApplicationInactivityTimeout evt) {
        ClientStreamIdentification clientStreamIdentification = evt.clientStreamIdentification();
        ClientComponent clientComponent = new ClientComponent(clientStreamIdentification.getClientStreamId(),
                                                              evt.client().clientId(),
                                                              evt.componentName(),
                                                              evt.client().context());
        String message = "Platform stream inactivity for " + clientStreamIdentification.getClientStreamId();
        ApplicationInactivityException exception = new ApplicationInactivityException(message);
        deregisterClient(clientComponent, exception);
    }

    /**
     * Return a {@link Set} of connected clients as {@link ClientComponent} instances.
     *
     * @return a {@link Set} of {@link ClientComponent} instances representing the connected clients.
     */
    public Set<ClientComponent> getConnectedClients() {
        return connectionMap.keySet();
    }

    /**
     * Finds all clients that are currently connected to the specified context and requests these client to reconnect.
     *
     * @param context the context
     */
    public void requestReconnectForContext(String context, String reason) {
        Set<ClientComponent> clients = connectionMap.keySet().stream().filter(c -> c.context.equals(context)).collect(
                Collectors.toSet());

        clients.forEach(clientName -> requestReconnect(clientName, reason));
    }

    /**
     * Functional interface describing a consumer of {@link PlatformOutboundInstruction}s to be called as a form of
     * handler functions when new instructions are received.
     */
    @FunctionalInterface
    public interface InstructionConsumer {

        /**
         * Consume the given {@code clientComponent} and {@link PlatformInboundInstruction}.
         *
         * @param client      the {@link ClientComponent} sending the instruction
         * @param instruction a {@link PlatformOutboundInstruction} describing the inbound instruction to be consumed
         */
        void accept(ClientComponent client, PlatformInboundInstruction instruction);
    }

    /**
     * Represent a client, specifying the {@code client} id, {@code component} name and the {@code context} of the
     * given client.
     */
    public static class ClientComponent implements Comparable<ClientComponent> {

        private final String clientStreamId;
        private final String clientId;
        private final String component;
        private final String context;

        /**
         * Creates an instance with specified parameters.
         *
         * @param clientStreamId the unique identifier of the client platform stream
         * @param clientId       the unique identifier of the client
         * @param component      the component name
         * @param context        the principal context of the client
         */
        public ClientComponent(String clientStreamId, String clientId, String component, String context) {
            this.clientStreamId = clientStreamId;
            this.clientId = clientId;
            this.component = component;
            this.context = context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClientComponent that = (ClientComponent) o;
            return clientStreamId.equals(that.clientStreamId) &&
                    context.equals(that.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientStreamId, context);
        }

        /**
         * Return the stream id of this client.
         *
         * @return a {@link String} representing the unique identifier of the platform connection of this client
         */
        public String getClientStreamId() {
            return clientStreamId;
        }

        /**
         * @return a {@link String} representing the unique identifier of this client
         */
        public String getClientId() {
            return clientId;
        }

        /**
         * Return the component name of this client.
         *
         * @return a {@link String} representing the component name of this client
         */
        public String getComponent() {
            return component;
        }

        /**
         * Return the context this client is working under.
         *
         * @return a {@link String} representing the context this client is working under
         */
        public String getContext() {
            return context;
        }

        @Override
        public String toString() {
            return "ClientComponent{" +
                    "client='" + clientStreamId + '\'' +
                    ", component='" + component + '\'' +
                    ", context='" + context + '\'' +
                    '}';
        }

        @Override
        public int compareTo(@NotNull ClientComponent o) {
            return clientStreamId.compareTo(o.clientStreamId);
        }
    }
}
