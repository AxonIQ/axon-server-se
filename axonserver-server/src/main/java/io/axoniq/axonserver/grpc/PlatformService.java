package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonserver.EventProcessorEvents.ProcessorStatusRequest;
import io.axoniq.axonserver.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonserver.TopologyEvents;
import io.axoniq.axonserver.topology.AxonServerNode;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.platform.grpc.ClientIdentification;
import io.axoniq.platform.grpc.NodeInfo;
import io.axoniq.platform.grpc.PauseEventProcessor;
import io.axoniq.platform.grpc.PlatformInboundInstruction;
import io.axoniq.platform.grpc.PlatformInboundInstruction.RequestCase;
import io.axoniq.platform.grpc.PlatformInfo;
import io.axoniq.platform.grpc.PlatformOutboundInstruction;
import io.axoniq.platform.grpc.PlatformServiceGrpc;
import io.axoniq.platform.grpc.ReleaseEventProcessorSegment;
import io.axoniq.platform.grpc.RequestEventProcessorInfo;
import io.axoniq.platform.grpc.RequestReconnect;
import io.axoniq.platform.grpc.StartEventProcessor;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * Author: marc
 */
@Service("PlatformService")
public class PlatformService extends PlatformServiceGrpc.PlatformServiceImplBase {
    private final Map<ClientComponent, SendingStreamObserver<PlatformOutboundInstruction>> connectionMap = new ConcurrentHashMap<>();
    private final Logger logger = LoggerFactory.getLogger(PlatformService.class);

    private final Topology clusterController;
    private final ContextProvider contextProvider;
    private final ApplicationEventPublisher eventPublisher;
    private final Map<RequestCase, Deque<InstructionConsumer>> handlers = new EnumMap<>(RequestCase.class);

    @FunctionalInterface
    public interface InstructionConsumer {
        void accept(String client, String context, PlatformInboundInstruction instruction);
    }

    public PlatformService(Topology clusterController,
                           ContextProvider contextProvider,
                           ApplicationEventPublisher eventPublisher) {
        this.clusterController = clusterController;
        this.contextProvider = contextProvider;
        this.eventPublisher = eventPublisher;
    }

//    private void notifyNewNode(ClusterEvent clusterNodeEvent) {
//        NodeInfo.Builder builder = NodeInfo.newBuilder()
//                .setHostName(clusterNodeEvent.getClusterNode().getHostName())
//                .setGrpcPort(clusterNodeEvent.getClusterNode().getGrpcPort())
//                .setVersion(0);
//        switch (clusterNodeEvent.getEventType()) {
//            case NODE_DELETED:
//                builder.setVersion(-1);
//                break;
//        }
//        connectionMap.values().forEach(streamObserver ->
//                streamObserver.onNext(PlatformOutboundInstruction.newBuilder()
//                        .setNodeNotification(builder)
//                        .build()));
//    }

    @Override
    public void getPlatformServer(ClientIdentification request, StreamObserver<PlatformInfo> responseObserver) {
        String context = contextProvider.getContext();
        try {
            AxonServerNode connectTo = clusterController.findNodeForClient(request.getClientName(),
                                                                           request.getComponentName(),
                                                                           context);
            responseObserver.onNext(PlatformInfo.newBuilder()
                                                .setPrimary(NodeInfo.newBuilder().setNodeName(connectTo.getName())
                                                                    .setHostName(connectTo.getHostName())
                                                                    .setGrpcPort(connectTo.getGrpcPort())
                                                                    .setHttpPort(connectTo.getHttpPort())
                                                ).build());
            responseObserver.onCompleted();
        } catch( RuntimeException cause) {
            logger.warn("Error processing client request {}", request, cause);
            responseObserver.onError(GrpcExceptionBuilder.build(cause));
        }
    }

    @Override
    public StreamObserver<PlatformInboundInstruction> openStream(StreamObserver<PlatformOutboundInstruction> responseObserver) {
        String context = contextProvider.getContext();
        SendingStreamObserver<PlatformOutboundInstruction> sendingStreamObserver = new SendingStreamObserver<>(responseObserver);
        return new ReceivingStreamObserver<PlatformInboundInstruction>(logger) {
            private ClientComponent clientComponent;
            private volatile ScheduledFuture<?> checkConnectionTask;
            @Override
            protected void consume(PlatformInboundInstruction instruction) {
                RequestCase requestCase = instruction.getRequestCase();
                handlers.getOrDefault(requestCase, new ArrayDeque<>())
                        .forEach(consumer -> consumer.accept(clientComponent.client, context, instruction));
                switch (requestCase) {
                    case REGISTER:
                        ClientIdentification client = instruction.getRegister();
                        clientComponent = new ClientComponent(client.getClientName(), client.getComponentName(), context);
                        registerClient(clientComponent, sendingStreamObserver);
                        break;
                    case REQUEST_NOT_SET:
                        break;
                }
            }

            @Override
            protected String sender() {
                return clientComponent == null ? null : clientComponent.client;
            }

            @Override
            public void onError(Throwable throwable) {
                stopConnectionCheck();
                deregisterClient(clientComponent);
            }

            private void stopConnectionCheck() {
                if( checkConnectionTask != null) {
                    checkConnectionTask.cancel(true);
                    checkConnectionTask = null;
                }
            }

            @Override
            public void onCompleted() {
                stopConnectionCheck();
                deregisterClient(clientComponent);
            }
        };
    }

//    private void rebalance() {
//        logger.debug("Rebalance: {}", connectionMap.keySet() );
//        connectionMap.keySet().stream().filter(e -> clusterController.canRebalance(e.client, e.component, e.context)).findFirst()
//                .ifPresent(this::requestReconnect);
//    }

    public boolean requestReconnect(ClientComponent clientName) {
        logger.debug("Request reconnect: {}", clientName);
        StreamObserver<PlatformOutboundInstruction> stream = connectionMap.get(clientName);
        if( stream != null) {
            stream.onNext(PlatformOutboundInstruction.newBuilder().setRequestReconnect(RequestReconnect.newBuilder()).build());
            return true;
        }
        return false;
    }

    public boolean requestReconnect(String client) {
        logger.debug("Request reconnect: {}", client);
        return connectionMap.entrySet().stream()
                .filter(e -> e.getKey().client.equals(client))
                .map(e -> requestReconnect(e.getKey()))
                .findFirst().orElse(false);
    }


    public void sendToClient(String clientName, PlatformOutboundInstruction instruction) {
        connectionMap.entrySet().stream()
                     .filter(e -> e.getKey().client.equals(clientName))
                     .map(Map.Entry::getValue)
                     .forEach(stream -> stream.onNext(instruction));
    }

    public void sendAllClients(PlatformOutboundInstruction instruction){
        connectionMap.values().forEach(stream -> stream.onNext(instruction));
    }

//    @EventListener
//    public void on(ContextEvents.ContextDeleted contextDeleted) {
//        connectionMap.entrySet().stream()
//                            .filter(e -> e.getKey().context.equals(contextDeleted.getName()))
//                            .forEach(e -> requestReconnect(e.getKey()));
//    }
//
//    @EventListener
//    public void on(ContextEvents.NodeDeletedFromContext nodeDeletedFromContext) {
//        if( this.configuration.getName().equals(nodeDeletedFromContext.getNode())) {
//            connectionMap.entrySet().stream()
//                         .filter(e -> e.getKey().context.equals(nodeDeletedFromContext.getName()))
//                         .forEach(e -> requestReconnect(e.getKey()));
//        }
//    }

    @EventListener
    public void onPauseEventProcessorRequest(PauseEventProcessorRequest evt){
        PlatformOutboundInstruction instruction = PlatformOutboundInstruction
                .newBuilder()
                .setPauseEventProcessor(PauseEventProcessor.newBuilder()
                                                           .setProcessorName(evt.processorName()))
                .build();
        this.sendToClient(evt.clientName(), instruction);
    }

    @EventListener
    public void onStartEventProcessorRequest(StartEventProcessorRequest evt){
        PlatformOutboundInstruction instruction = PlatformOutboundInstruction
                .newBuilder()
                .setStartEventProcessor(StartEventProcessor.newBuilder().setProcessorName(evt.processorName()))
                .build();
        this.sendToClient(evt.clientName(), instruction);
    }

    @EventListener
    public void on(ReleaseSegmentRequest evt){
        PlatformOutboundInstruction instruction = PlatformOutboundInstruction
                .newBuilder()
                .setReleaseSegment(ReleaseEventProcessorSegment.newBuilder()
                                                               .setProcessorName(evt.processorName())
                                                               .setSegmentIdentifier(evt.segmentId()))
                .build();
        this.sendToClient(evt.clientName(), instruction);
    }

    @EventListener
    public void on(ProcessorStatusRequest evt){
        PlatformOutboundInstruction instruction = PlatformOutboundInstruction
                .newBuilder()
                .setRequestEventProcessorInfo(RequestEventProcessorInfo.newBuilder()
                                                                       .setProcessorName(evt.processorName()))
                .build();
        this.sendToClient(evt.clientName(), instruction);
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        StreamObserver<PlatformOutboundInstruction> connection = connectionMap.remove(new ClientComponent(event.getClient(), event.getComponentName(), event.getContext()));
        logger.debug("application disconnected: {}, connection: {}", event.getClient(), connection);
        if( connection != null) {
            try {
                connection.onCompleted();
            } catch (Exception ex ) {
                logger.debug("Error while closing tracking event processor connection from {} - {}", event.getClient(), ex.getMessage());
            }
        }
    }

    public void onInboundInstruction(RequestCase requestCase, InstructionConsumer consumer) {
        Deque<InstructionConsumer> consumers = handlers.computeIfAbsent(requestCase,rc -> new ArrayDeque<>());
        consumers.add(consumer);
    }


    private void registerClient(ClientComponent clientComponent,
                                SendingStreamObserver<PlatformOutboundInstruction> responseObserver){
        connectionMap.put(clientComponent, responseObserver);
        logger.debug("Registered client : {}", clientComponent);
//        if(limits.isClusterAutobalancingEnabled() && reconnectTask == null) {
//            ClusterConfiguration conf = configuration.getCluster();
//            reconnectTask = scheduler.scheduleAtFixedRate(this::rebalance, conf.getRebalanceDelay(), conf.getRebalanceInterval(), SECONDS);
//        }
        eventPublisher.publishEvent(new TopologyEvents.ApplicationConnected(clientComponent.context,
                                                                            clientComponent.component, clientComponent.client));
    }

    private void deregisterClient(ClientComponent cc){
        logger.debug("De-registered client : {}", cc);
        if( cc != null)  {
            connectionMap.remove(cc);
            eventPublisher.publishEvent(new TopologyEvents.ApplicationDisconnected(cc.context,cc.component, cc.client, null));
        }

    }


    private class ClientComponent {

        private final String client;
        private final String component;
        private final String context;

        private ClientComponent(String client, String component, String context) {
            this.client = client;
            this.component = component;
            this.context = context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClientComponent that = (ClientComponent) o;
            return Objects.equals(client, that.client);
        }

        @Override
        public int hashCode() {
            return Objects.hash(client);
        }

        @Override
        public String toString() {
            return "ClientComponent{" +
                    "client='" + client + '\'' +
                    '}';
        }
    }
}
