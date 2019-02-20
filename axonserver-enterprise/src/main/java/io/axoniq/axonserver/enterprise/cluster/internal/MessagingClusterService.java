package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.MetricsEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ClientEventProcessorStatusProtoConverter;
import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.ReceivingStreamObserver;
import io.axoniq.axonserver.grpc.SendingStreamObserver;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessor;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorSegment;
import io.axoniq.axonserver.grpc.internal.ClientStatus;
import io.axoniq.axonserver.grpc.internal.CommandHandlerStatus;
import io.axoniq.axonserver.grpc.internal.ConnectResponse;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand.RequestCase;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.Group;
import io.axoniq.axonserver.grpc.internal.MessagingClusterServiceGrpc;
import io.axoniq.axonserver.grpc.internal.QueryHandlerStatus;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;
import javax.annotation.PreDestroy;

/**
 * Handles requests from other axonserver cluster servers acting as message processors.
 * Other servers connect to this service to receive commands and queries.
 * When 2 nodes are connected there are 2 connections to forward messages to both nodes.
 * Client side is implemented in {@link RemoteConnection}
 *
 * Request sequence;
 * Connect
 * FlowControl
 * And then subscriptions (queries, commands)
 *
 * Maintains a list of clients connected to connected service.
 *
 * When connection lost, already sent commands are returned to caller with error status
 * @author Marc Gathier
 */
@Service("MessagingClusterService")
public class MessagingClusterService extends MessagingClusterServiceGrpc.MessagingClusterServiceImplBase {
    private final Logger logger = LoggerFactory.getLogger(MessagingClusterService.class);
    private final CommandDispatcher commandDispatcher;
    private final QueryDispatcher queryDispatcher;
    private final ClusterController clusterController;
    private final ApplicationEventPublisher eventPublisher;
    private final Map<String, ConnectorReceivingStreamObserver> connections = new ConcurrentHashMap<>();
    private final Map<RequestCase, Collection<BiConsumer<ConnectorCommand, Publisher<ConnectorResponse>>>> handlers
            = new EnumMap<>(RequestCase.class);


    @Value("${axoniq.axonserver.cluster.connectionCheckRetries:5}")
    private int connectionCheckRetries = 5;
    @Value("${axoniq.axonserver.cluster.connectionCheckRetryWait:1000}")
    private int connectionCheckRetryWait = 1000;
    @Value("${axoniq.axonserver.cluster.query-threads:1}")
    private int queryProcessingThreads = 1;
    @Value("${axoniq.axonserver.cluster.command-threads:1}")
    private int commandProcessingThreads = 1;
    private final Set<GrpcFlowControlledDispatcherListener> dispatchListeners = new CopyOnWriteArraySet<>();


    public MessagingClusterService(
            CommandDispatcher commandDispatcher,
            QueryDispatcher queryDispatcher,
            ClusterController clusterController,
            ApplicationEventPublisher eventPublisher) {
        this.commandDispatcher = commandDispatcher;
        this.queryDispatcher = queryDispatcher;
        this.clusterController = clusterController;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public StreamObserver<ConnectorCommand> openStream(StreamObserver<ConnectorResponse> responseObserver1) {
        SendingStreamObserver<ConnectorResponse> responseObserver = new SendingStreamObserver<>(responseObserver1);
        return new ConnectorReceivingStreamObserver(responseObserver);
    }


    @PreDestroy
    public void shutdown() {
        dispatchListeners.forEach(GrpcFlowControlledDispatcherListener::cancel);
        dispatchListeners.clear();
    }

    Set<GrpcFlowControlledDispatcherListener> listeners() {
        return dispatchListeners;
    }

    private class ConnectorReceivingStreamObserver extends ReceivingStreamObserver<ConnectorCommand> {

        private final CopyOnWriteArraySet<ClientIdentification> clients;
        private final SendingStreamObserver<ConnectorResponse> responseObserver;
        private volatile GrpcInternalCommandDispatcherListener commandQueueListener;
        private volatile GrpcInternalQueryDispatcherListener queryQueueListener;
        private volatile String messagingServerName;
        private final Map<ClientIdentification, CommandHandler> commandHandlerPerContextClient = new ConcurrentHashMap<>();
        private final Map<ClientIdentification, QueryHandler> queryHandlerPerContextClient = new ConcurrentHashMap<>();

        public ConnectorReceivingStreamObserver(SendingStreamObserver<ConnectorResponse> responseObserver) {
            super(logger);
            this.responseObserver = responseObserver;
            clients = new CopyOnWriteArraySet<>();
        }

        @Override
        protected void consume(ConnectorCommand connectorCommand) {
            handlers.getOrDefault(connectorCommand.getRequestCase(), Collections.emptySet())
                    .forEach(consumer -> consumer.accept(connectorCommand, responseObserver::onNext));
                switch (connectorCommand.getRequestCase()) {
                    case CONNECT:
                        try {
                            messagingServerName = connectorCommand.getConnect().getNodeInfo().getNodeName();
                            if( clusterController.connect(connectorCommand.getConnect().getNodeInfo(), connectorCommand.getConnect().getAdmin()) ) {
                                logger.debug("Received connect from: {} - {}",
                                             messagingServerName,
                                             connectorCommand.getConnect());

                                responseObserver.onNext(ConnectorResponse.newBuilder()
                                                                         .setConnectResponse(ConnectResponse
                                                                                                     .newBuilder())
                                                                         .build());
                                connections.put(messagingServerName, this);
                            } else {
                                logger.warn("Received connection from unknown node {}, closing connection", messagingServerName);
                                responseObserver.onNext(ConnectorResponse.newBuilder()
                                                                         .setConnectResponse(ConnectResponse
                                                                                                     .newBuilder().setDeleted(true))
                                                                         .build());
                                responseObserver.onCompleted();
                            }
                        } catch( MessagingPlatformException mpe) {
                            responseObserver.onError(mpe);
                        }
                        break;
                    case SUBSCRIBE_COMMAND:
                        CommandSubscription command = connectorCommand.getSubscribeCommand()
                                                                      .getCommand();
                        logger.debug("SUBSCRIBE [{}] [{}] [{}]", command.getCommand(),
                                     command.getClientId(),
                                     messagingServerName);

                        checkClient(connectorCommand.getSubscribeCommand().getContext(),
                                    command.getComponentName(),
                                    command.getClientId());

                        CommandHandler commandHandler = commandHandlerPerContextClient
                                .computeIfAbsent(new ClientIdentification(connectorCommand.getSubscribeCommand().getContext(),
                                                                          command.getClientId()), clientIdentification -> new
                                        ProxyCommandHandler(responseObserver, clientIdentification,
                                                            command.getComponentName(),
                                                            messagingServerName));
                        eventPublisher.publishEvent(new SubscriptionEvents.SubscribeCommand(commandHandler.getClient().getContext(),
                                                                                            command, commandHandler));
                        break;
                    case UNSUBSCRIBE_COMMAND:
                        logger.debug("UNSUBSCRIBE [{}] [{}] [{}]",
                                     connectorCommand.getUnsubscribeCommand().getCommand(),
                                     connectorCommand.getUnsubscribeCommand().getCommand().getClientId(),
                                     messagingServerName);
                        eventPublisher.publishEvent(new SubscriptionEvents.UnsubscribeCommand(
                                connectorCommand.getUnsubscribeCommand().getContext(),
                                connectorCommand.getUnsubscribeCommand().getCommand(),
                                true));
                        break;
                    case COMMAND_RESPONSE:
                        logger.debug("Received command response {} from: {}", connectorCommand.getCommandResponse(),
                                     messagingServerName);
                        commandDispatcher.handleResponse(new SerializedCommandResponse(connectorCommand.getCommandResponse().getRequestIdentifier(), connectorCommand.getCommandResponse().getResponse().toByteArray()), true);
                        break;
                    case SUBSCRIBE_QUERY:
                        QuerySubscription query = connectorCommand.getSubscribeQuery().getQuery();
                        logger.debug("SUBSCRIBE [{}/{}] [{}] [{}]", query.getQuery(),
                                     query.getResultName(),
                                     query.getClientId(),
                                     messagingServerName);
                        checkClient(connectorCommand.getSubscribeQuery().getContext(),
                                    query.getComponentName(),
                                    query.getClientId());

                        ClientIdentification clientIdentification = new ClientIdentification(
                                connectorCommand.getSubscribeQuery().getContext(),
                                query.getClientId());
                        QueryHandler queryHandler = queryHandlerPerContextClient.computeIfAbsent(clientIdentification,
                                                                                                 contextClient -> new ProxyQueryHandler(responseObserver,
                                                                                             clientIdentification,
                                                                                             query.getComponentName(),
                                                                                             messagingServerName));
                        eventPublisher.publishEvent(new SubscriptionEvents.SubscribeQuery(connectorCommand
                                                                                                  .getSubscribeQuery()
                                                                                                  .getContext(),
                                                                                          query, queryHandler));

                        break;
                    case UNSUBSCRIBE_QUERY:
                        logger.debug("UNSUBSCRIBE [{}/{}] [{}] [{}]",
                                     connectorCommand.getUnsubscribeQuery().getQuery().getQuery(),
                                     connectorCommand.getUnsubscribeQuery().getQuery().getResultName(),
                                     connectorCommand.getUnsubscribeQuery().getQuery().getClientId(),
                                     messagingServerName);
                        eventPublisher.publishEvent(new SubscriptionEvents.UnsubscribeQuery(connectorCommand
                                                                                                    .getUnsubscribeQuery()
                                                                                                    .getContext(),
                                                                                            connectorCommand
                                                                                                    .getUnsubscribeQuery()
                                                                                                    .getQuery(),
                                                                                            true));
                        break;
                    case QUERY_RESPONSE:
                        if( logger.isDebugEnabled()) logger.debug("QUERY_RESPONSE {} from {}",
                                     connectorCommand.getQueryResponse().getRequestIdentifier(),
                                     ProcessingInstructionHelper.targetClient(connectorCommand.getQueryResponse()
                                                                                              .getProcessingInstructionsList()));
                        queryDispatcher.handleResponse(connectorCommand.getQueryResponse()
                                , ProcessingInstructionHelper.targetClient(connectorCommand.getQueryResponse().getProcessingInstructionsList()),
                                                                                             true);
                        break;
                    case QUERY_COMPLETE:
                        logger.debug("QUERY_COMPLETE {} from {}",
                                     connectorCommand.getQueryComplete().getMessageId(),
                                     connectorCommand.getQueryComplete().getClient());
                        queryDispatcher.handleComplete(connectorCommand.getQueryComplete().getClient(),
                                                       connectorCommand.getQueryComplete().getMessageId(),
                                                       true);
                        break;
                    case FLOW_CONTROL:
                        logger.debug("FLOW_CONTROL {}", connectorCommand.getFlowControl());
                        handleFlowControl(connectorCommand);
                        break;
                    case METRICS:
                        eventPublisher.publishEvent(new MetricsEvents.MetricsChanged(connectorCommand
                                                                                             .getMetrics()));
                        break;
                    case CLIENT_STATUS:
                        updateClientStatus(connectorCommand.getClientStatus());
                        break;
                    case QUERY_HANDLER_STATUS:
                        QueryHandlerStatus queryHandlerStatus = connectorCommand.getQueryHandlerStatus();
                        if (!queryHandlerStatus.getConnected()){
                            eventPublisher.publishEvent(new TopologyEvents.QueryHandlerDisconnected(queryHandlerStatus.getContext(), queryHandlerStatus.getClientName(), true));
                        }
                        break;
                    case COMMAND_HANDLER_STATUS:
                        CommandHandlerStatus commandHandlerStatus = connectorCommand.getCommandHandlerStatus();
                        if (!commandHandlerStatus.getConnected()){
                            eventPublisher.publishEvent(new TopologyEvents.CommandHandlerDisconnected(commandHandlerStatus.getContext(), commandHandlerStatus.getClientName(), true));
                        }
                        break;
                    case CLIENT_EVENT_PROCESSOR_STATUS:
                        eventPublisher.publishEvent(
                                new EventProcessorEvents.EventProcessorStatusUpdate(ClientEventProcessorStatusProtoConverter
                                                                       .fromProto(connectorCommand.getClientEventProcessorStatus()),
                                                                                    true));
                        break;
                    case START_CLIENT_EVENT_PROCESSOR:
                        ClientEventProcessor startProcessor = connectorCommand.getStartClientEventProcessor();
                        eventPublisher.publishEvent(
                                new EventProcessorEvents.StartEventProcessorRequest(startProcessor.getClient(),
                                                                                    startProcessor.getProcessorName(), true));
                        break;
                    case PAUSE_CLIENT_EVENT_PROCESSOR:
                        ClientEventProcessor pauseProcessor = connectorCommand.getPauseClientEventProcessor();
                        eventPublisher.publishEvent(
                                new EventProcessorEvents.PauseEventProcessorRequest(pauseProcessor.getClient(),
                                                                                    pauseProcessor.getProcessorName(), true));
                        break;
                    case RELEASE_SEGMENT:
                        ClientEventProcessorSegment releaseSegment = connectorCommand.getReleaseSegment();
                        eventPublisher.publishEvent(new EventProcessorEvents.ReleaseSegmentRequest(releaseSegment.getClient(),
                                                                                                   releaseSegment.getProcessorName(),
                                                                                                   releaseSegment.getSegmentIdentifier(),
                                                                                                   true));
                        break;
                    case REQUEST_PROCESSOR_STATUS:
                        ClientEventProcessor requestStatus = connectorCommand.getRequestProcessorStatus();
                        eventPublisher.publishEvent(new EventProcessorEvents.ProcessorStatusRequest(requestStatus.getClient(),
                                                                                                    requestStatus.getProcessorName(),
                                                                                                    true));
                        break;
                    case SUBSCRIPTION_QUERY_RESPONSE:
                            SubscriptionQueryResponse response = connectorCommand.getSubscriptionQueryResponse();
                            eventPublisher.publishEvent(new SubscriptionQueryEvents.SubscriptionQueryResponseReceived(response));
                            break;
                    default:
                        break;
                }
        }

        private void handleFlowControl(ConnectorCommand connectorCommand) {
            if (Group.COMMAND.equals(connectorCommand.getFlowControl().getGroup())) {
                if (commandQueueListener == null) {
                    commandQueueListener = new GrpcInternalCommandDispatcherListener(commandDispatcher
                                                                                             .getCommandQueues(),
                                                                                     connectorCommand
                                                                                             .getFlowControl()
                                                                                             .getNodeName(),
                                                                                     responseObserver, commandProcessingThreads);

                    dispatchListeners.add(commandQueueListener);
                }
                commandQueueListener.addPermits(connectorCommand.getFlowControl().getPermits());
            }
            if (Group.QUERY.equals(connectorCommand.getFlowControl().getGroup())) {
                if (queryQueueListener == null) {
                    queryQueueListener = new GrpcInternalQueryDispatcherListener(queryDispatcher,
                                                                                 connectorCommand
                                                                                         .getFlowControl()
                                                                                         .getNodeName(),
                                                                                 responseObserver, queryProcessingThreads);
                }
                queryQueueListener.addPermits(connectorCommand.getFlowControl().getPermits());
                dispatchListeners.add(queryQueueListener);
            }
        }

        @Override
        protected String sender() {
            return messagingServerName;
        }

        private void checkClient(String context, String component, String clientName) {
            if( clients.add(new ClientIdentification(context, clientName))) {
                eventPublisher.publishEvent(new TopologyEvents.ApplicationConnected(context,
                                                                                    component,
                                                                                    clientName,
                                                                                    messagingServerName));
            }
        }

        private void updateClientStatus(ClientStatus clientStatus) {
            ClientIdentification clientIdentification = new ClientIdentification(clientStatus.getContext(), clientStatus.getClientName());
            if( clientStatus.getConnected()) {

                if( clients.add(clientIdentification) ){
                    // unknown client
                    eventPublisher.publishEvent(new TopologyEvents.ApplicationConnected(clientStatus.getContext(),
                                                                                       clientStatus.getComponentName(),
                                                                                       clientStatus.getClientName(),
                                                                                       messagingServerName));
                }
            } else {
                if( clients.remove(clientIdentification)) {
                    // known client
                    logger.info("Client disconnected: {}", clientStatus.getClientName());
                    commandHandlerPerContextClient.remove(clientIdentification);
                    queryHandlerPerContextClient.remove(clientIdentification);
                    eventPublisher.publishEvent(new TopologyEvents.ApplicationDisconnected(clientStatus.getContext(),
                                                                                       clientStatus.getComponentName(),
                                                                                       clientStatus.getClientName(),
                                                                                       messagingServerName));
                }
            }
        }


        @Override
        public void onError(Throwable throwable) {
            logger.info("{}: Error on connection from AxonHub node - {}", messagingServerName, throwable.getMessage());
            closeConnections();
        }

        @Override
        public void onCompleted() {
            closeConnections();
        }

        private void closeConnections() {
            if( messagingServerName != null) {
                connections.remove(messagingServerName);
            }
            if (commandQueueListener != null) {
                commandQueueListener.cancel();
                dispatchListeners.remove(commandQueueListener);
                commandQueueListener = null;
            }
            if (queryQueueListener != null) {
                queryQueueListener.cancel();
                dispatchListeners.remove(queryQueueListener);
                queryQueueListener = null;
            }
            clients.forEach(client -> eventPublisher.publishEvent(new TopologyEvents.ApplicationDisconnected(client.getContext(),
                                                                                                        null,
                                                                                                        client.getClient(),
                                                                                                        messagingServerName)));
            eventPublisher.publishEvent(new ClusterEvents.AxonServerInstanceDisconnected(messagingServerName));
        }

        public void publish(ConnectorResponse connectorResponse) {
            responseObserver.onNext(connectorResponse);
        }
    }
}
