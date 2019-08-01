package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.MetricsEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import io.axoniq.axonserver.grpc.ReceivingStreamObserver;
import io.axoniq.axonserver.grpc.SendingStreamObserver;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessor;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorSegment;
import io.axoniq.axonserver.grpc.internal.ClientStatus;
import io.axoniq.axonserver.grpc.internal.CommandHandlerStatus;
import io.axoniq.axonserver.grpc.internal.ConnectRequest;
import io.axoniq.axonserver.grpc.internal.ConnectResponse;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.ForwardedCommandResponse;
import io.axoniq.axonserver.grpc.internal.ForwardedQueryResponse;
import io.axoniq.axonserver.grpc.internal.Group;
import io.axoniq.axonserver.grpc.internal.InternalCommandSubscription;
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
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import javax.annotation.PreDestroy;

import static io.axoniq.axonserver.enterprise.cluster.internal.ForwardedQueryResponseUtils.getDispatchingClient;
import static io.axoniq.axonserver.enterprise.cluster.internal.ForwardedQueryResponseUtils.unwrap;
import static io.axoniq.axonserver.grpc.ClientEventProcessorStatusProtoConverter.fromProto;

/**
 * Handles requests from other Axon Server cluster servers acting as message processors. Other servers connect to this
 * service to receive commands and queries. When 2 nodes are connected there are 2 connections to forward messages to
 * both nodes. Client side is implemented in {@link RemoteConnection}
 * <p>
 * Request sequence;
 * Connect
 * FlowControl
 * And then subscriptions (queries, commands)
 * <p>
 * Maintains a list of clients connected to connected service.
 * <p>
 * When connection lost, already sent commands are returned to caller with error status
 *
 * @author Marc Gathier
 * @since 4.0
 */
@Service("MessagingClusterService")
public class MessagingClusterService extends MessagingClusterServiceGrpc.MessagingClusterServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(MessagingClusterService.class);

    private final CommandDispatcher commandDispatcher;
    private final QueryDispatcher queryDispatcher;
    private final ClusterController clusterController;
    private final ApplicationEventPublisher eventPublisher;


    private final Map<ClientIdentification, String> connectedClients = new ConcurrentHashMap<>();

    @Value("${axoniq.axonserver.cluster.connectionCheckRetries:5}")
    private int connectionCheckRetries = 5;
    @Value("${axoniq.axonserver.cluster.connectionCheckRetryWait:1000}")
    private int connectionCheckRetryWait = 1000;
    @Value("${axoniq.axonserver.cluster.query-threads:1}")
    private int queryProcessingThreads = 1;
    @Value("${axoniq.axonserver.cluster.command-threads:1}")
    private int commandProcessingThreads = 1;
    private final Set<GrpcFlowControlledDispatcherListener> dispatchListeners = new CopyOnWriteArraySet<>();

    /**
     * Instantiate a {@link MessagingClusterService} which consumes all incoming messages propagated between a cluster
     * of Axon Server instances.
     *
     * @param commandDispatcher the {@link CommandDispatcher} used to dispatch commands and command responses
     * @param queryDispatcher   the {@link QueryDispatcher} used to dispatch queries, query responses and subscription
     *                          query updates
     * @param clusterController the {@link ClusterController} used to add new nodes trying to connect to the cluster
     * @param eventPublisher    the {@link ApplicationEventPublisher} to publish events through this Axon Server
     *                          instance
     */
    public MessagingClusterService(CommandDispatcher commandDispatcher,
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

    /**
     * Shutdown this {@link MessagingClusterService}, by canceling all the dispatch listeners.
     */
    @PreDestroy
    public void shutdown() {
        dispatchListeners.forEach(GrpcFlowControlledDispatcherListener::cancel);
        dispatchListeners.clear();
    }

    Set<GrpcFlowControlledDispatcherListener> listeners() {
        return dispatchListeners;
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected applicationDisconnected) {
        String axonServerNode = applicationDisconnected.isProxied() ? applicationDisconnected.getProxy() : "LOCAL";
        connectedClients.computeIfPresent(new ClientIdentification(applicationDisconnected.getContext(), applicationDisconnected.getClient()),
                                          (client,current)-> current.equals(axonServerNode) ? null : current);
    }

    @EventListener
    public void on(TopologyEvents.ApplicationConnected applicationConnected) {
        String axonServerNode = applicationConnected.isProxied() ? applicationConnected.getProxy() : "LOCAL";
        connectedClients.put(new ClientIdentification(applicationConnected.getContext(), applicationConnected.getClient()), axonServerNode);
    }

    private class ConnectorReceivingStreamObserver extends ReceivingStreamObserver<ConnectorCommand> {

        private static final boolean PROXIED = true;

        private final SendingStreamObserver<ConnectorResponse> responseObserver;
        private volatile GrpcInternalCommandDispatcherListener commandQueueListener;
        private volatile GrpcInternalQueryDispatcherListener queryQueueListener;
        private volatile String messagingServerName;
        private final Map<ClientIdentification, CommandHandler> commandHandlerPerContextClient = new ConcurrentHashMap<>();
        private final Map<ClientIdentification, QueryHandler> queryHandlerPerContextClient = new ConcurrentHashMap<>();

        private ConnectorReceivingStreamObserver(SendingStreamObserver<ConnectorResponse> responseObserver) {
            super(logger);
            this.responseObserver = responseObserver;
        }

        @Override
        protected void consume(ConnectorCommand connectorCommand) {
            switch (connectorCommand.getRequestCase()) {
                case CONNECT:
                    try {
                        ConnectRequest connectRequest = connectorCommand.getConnect();
                        messagingServerName = connectRequest.getNodeInfo().getNodeName();

                        if (clusterController.connect(connectRequest.getNodeInfo(), connectRequest.getAdmin())) {
                            logger.debug("Received connect from: {} - {}", messagingServerName, connectRequest);

                            responseObserver.onNext(ConnectorResponse.newBuilder()
                                                                     .setConnectResponse(ConnectResponse.newBuilder())
                                                                     .build());
                        } else {
                            logger.warn(
                                    "Received connection from unknown node {}, closing connection", messagingServerName
                            );

                            responseObserver.onNext(
                                    ConnectorResponse.newBuilder()
                                                     .setConnectResponse(ConnectResponse.newBuilder().setDeleted(true))
                                                     .build()
                            );
                            responseObserver.onCompleted();
                        }
                    } catch (MessagingPlatformException mpe) {
                        responseObserver.onError(mpe);
                    }
                    break;
                case SUBSCRIBE_COMMAND:
                    InternalCommandSubscription subscribeCommand = connectorCommand.getSubscribeCommand();
                    CommandSubscription command = subscribeCommand.getCommand();
                    String componentName = command.getComponentName();
                    String clientId = command.getClientId();

                    logger.debug(
                            "SUBSCRIBE [{}] [{}] [{}]",
                            command.getCommand(), clientId, messagingServerName
                    );

                    checkClient(subscribeCommand.getContext(), componentName, clientId);

                    CommandHandler commandHandler = commandHandlerPerContextClient.computeIfAbsent(
                            new ClientIdentification(subscribeCommand.getContext(), clientId),
                            clientIdentification -> new ProxyCommandHandler(
                                    responseObserver,
                                    clientIdentification,
                                    componentName,
                                    messagingServerName
                            )
                    );
                    eventPublisher.publishEvent(new SubscriptionEvents.SubscribeCommand(
                            commandHandler.getClient().getContext(), command, commandHandler
                    ));
                    break;
                case UNSUBSCRIBE_COMMAND:
                    InternalCommandSubscription unsubscribeCommand = connectorCommand.getUnsubscribeCommand();
                    CommandSubscription commandSubscription = unsubscribeCommand.getCommand();

                    logger.debug(
                            "UNSUBSCRIBE [{}] [{}] [{}]",
                            commandSubscription, commandSubscription.getClientId(), messagingServerName
                    );

                    eventPublisher.publishEvent(new SubscriptionEvents.UnsubscribeCommand(
                            unsubscribeCommand.getContext(), commandSubscription, PROXIED
                    ));
                    break;
                case COMMAND_RESPONSE:
                    ForwardedCommandResponse commandResponse = connectorCommand.getCommandResponse();
                    logger.debug("Received command response {} from: {}", commandResponse, messagingServerName);

                    commandDispatcher.handleResponse(
                            new SerializedCommandResponse(commandResponse.getRequestIdentifier(),
                                                          commandResponse.getResponse().toByteArray()),
                            PROXIED
                    );
                    break;
                case SUBSCRIBE_QUERY:
                    QuerySubscription query = connectorCommand.getSubscribeQuery().getQuery();
                    String queryContext = connectorCommand.getSubscribeQuery().getContext();
                    logger.debug(
                            "SUBSCRIBE [{}/{}] [{}] [{}]", query.getQuery(),
                            query.getResultName(), query.getClientId(), messagingServerName
                    );

                    checkClient(queryContext, query.getComponentName(), query.getClientId());
                    ClientIdentification clientIdentification =
                            new ClientIdentification(queryContext, query.getClientId());

                    QueryHandler queryHandler = queryHandlerPerContextClient.computeIfAbsent(
                            clientIdentification,
                            contextClient -> new ProxyQueryHandler(
                                    responseObserver,
                                    clientIdentification,
                                    query.getComponentName(),
                                    messagingServerName
                            )
                    );

                    eventPublisher.publishEvent(new SubscriptionEvents.SubscribeQuery(
                            queryContext, query, queryHandler
                    ));
                    break;
                case UNSUBSCRIBE_QUERY:
                    QuerySubscription querySubscription = connectorCommand.getUnsubscribeQuery().getQuery();
                    logger.debug(
                            "UNSUBSCRIBE [{}/{}] [{}] [{}]",
                            querySubscription.getQuery(),
                            querySubscription.getResultName(),
                            querySubscription.getClientId(),
                            messagingServerName
                    );

                    eventPublisher.publishEvent(new SubscriptionEvents.UnsubscribeQuery(
                            connectorCommand.getUnsubscribeQuery().getContext(),
                            querySubscription,
                            PROXIED
                    ));
                    break;
                case QUERY_RESPONSE:
                    ForwardedQueryResponse queryResponse = connectorCommand.getQueryResponse();
                    String respondingClientId = getDispatchingClient(queryResponse);
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "QUERY_RESPONSE {} from {}",
                                queryResponse.getRequestIdentifier(),
                                respondingClientId
                        );
                    }

                    queryDispatcher.handleResponse(unwrap(queryResponse), respondingClientId, PROXIED);
                    break;
                case QUERY_COMPLETE:
                    logger.debug(
                            "QUERY_COMPLETE {} from {}",
                            connectorCommand.getQueryComplete().getMessageId(),
                            connectorCommand.getQueryComplete().getClient()
                    );

                    queryDispatcher.handleComplete(
                            connectorCommand.getQueryComplete().getMessageId(),
                            connectorCommand.getQueryComplete().getClient(),
                            PROXIED
                    );
                    break;
                case FLOW_CONTROL:
                    logger.debug("FLOW_CONTROL {}", connectorCommand.getFlowControl());

                    handleFlowControl(connectorCommand);
                    break;
                case METRICS:
                    eventPublisher.publishEvent(new MetricsEvents.MetricsChanged(connectorCommand.getMetrics()));
                    break;
                case CLIENT_STATUS:
                    updateClientStatus(connectorCommand.getClientStatus());
                    break;
                case QUERY_HANDLER_STATUS:
                    QueryHandlerStatus queryHandlerStatus = connectorCommand.getQueryHandlerStatus();
                    if (!queryHandlerStatus.getConnected()) {
                        eventPublisher.publishEvent(new TopologyEvents.QueryHandlerDisconnected(
                                queryHandlerStatus.getContext(), queryHandlerStatus.getClientName(), PROXIED
                        ));
                    }
                    break;
                case COMMAND_HANDLER_STATUS:
                    CommandHandlerStatus commandHandlerStatus = connectorCommand.getCommandHandlerStatus();
                    if (!commandHandlerStatus.getConnected()) {
                        eventPublisher.publishEvent(new TopologyEvents.CommandHandlerDisconnected(
                                commandHandlerStatus.getContext(), commandHandlerStatus.getClientName(), PROXIED
                        ));
                    }
                    break;
                case CLIENT_EVENT_PROCESSOR_STATUS:
                    eventPublisher.publishEvent(new EventProcessorEvents.EventProcessorStatusUpdate(
                            fromProto(connectorCommand.getClientEventProcessorStatus()), PROXIED
                    ));
                    break;
                case START_CLIENT_EVENT_PROCESSOR:
                    ClientEventProcessor startProcessor = connectorCommand.getStartClientEventProcessor();
                    eventPublisher.publishEvent(new EventProcessorEvents.StartEventProcessorRequest(
                            startProcessor.getClient(), startProcessor.getProcessorName(), PROXIED
                    ));
                    break;
                case PAUSE_CLIENT_EVENT_PROCESSOR:
                    ClientEventProcessor pauseProcessor = connectorCommand.getPauseClientEventProcessor();
                    eventPublisher.publishEvent(new EventProcessorEvents.PauseEventProcessorRequest(
                            pauseProcessor.getClient(), pauseProcessor.getProcessorName(), PROXIED
                    ));
                    break;
                case RELEASE_SEGMENT:
                    ClientEventProcessorSegment releaseSegment = connectorCommand.getReleaseSegment();
                    eventPublisher.publishEvent(new EventProcessorEvents.ReleaseSegmentRequest(
                            releaseSegment.getClient(),
                            releaseSegment.getProcessorName(),
                            releaseSegment.getSegmentIdentifier(),
                            PROXIED
                    ));
                    break;
                case REQUEST_PROCESSOR_STATUS:
                    ClientEventProcessor requestStatus = connectorCommand.getRequestProcessorStatus();
                    eventPublisher.publishEvent(new EventProcessorEvents.ProcessorStatusRequest(
                            requestStatus.getClient(), requestStatus.getProcessorName(), PROXIED
                    ));
                    break;
                case SUBSCRIPTION_QUERY_RESPONSE:
                    SubscriptionQueryResponse response = connectorCommand.getSubscriptionQueryResponse();
                    eventPublisher.publishEvent(
                            new SubscriptionQueryEvents.SubscriptionQueryResponseReceived(response)
                    );
                    break;
                case SPLIT_SEGMENT:
                    ClientEventProcessorSegment splitSegment = connectorCommand.getSplitSegment();
                    eventPublisher.publishEvent(new SplitSegmentRequest(
                            PROXIED, splitSegment.getClient(), splitSegment.getProcessorName(),
                            splitSegment.getSegmentIdentifier()
                    ));
                    break;
                case MERGE_SEGMENT:
                    ClientEventProcessorSegment mergeSegment = connectorCommand.getMergeSegment();
                    eventPublisher.publishEvent(new MergeSegmentRequest(
                            PROXIED, mergeSegment.getClient(), mergeSegment.getProcessorName(),
                            mergeSegment.getSegmentIdentifier()
                    ));
                    break;
                default:
                    logger.warn("Unknown operation occurred [{}]", connectorCommand.getRequestCase());
                    break;
            }
        }

        private void handleFlowControl(ConnectorCommand connectorCommand) {
            if (Group.COMMAND.equals(connectorCommand.getFlowControl().getGroup())) {
                if (commandQueueListener == null) {
                    commandQueueListener = new GrpcInternalCommandDispatcherListener(
                            commandDispatcher.getCommandQueues(),
                            connectorCommand.getFlowControl().getNodeName(),
                            responseObserver,
                            commandProcessingThreads
                    );
                    dispatchListeners.add(commandQueueListener);
                }
                commandQueueListener.addPermits(connectorCommand.getFlowControl().getPermits());
            }
            if (Group.QUERY.equals(connectorCommand.getFlowControl().getGroup())) {
                if (queryQueueListener == null) {
                    queryQueueListener = new GrpcInternalQueryDispatcherListener(
                            queryDispatcher,
                            connectorCommand.getFlowControl().getNodeName(),
                            responseObserver,
                            queryProcessingThreads
                    );
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
            if (messagingServerName != null && !messagingServerName.equals(connectedClients.get((new ClientIdentification(context, clientName))))) {
                eventPublisher.publishEvent(new TopologyEvents.ApplicationConnected(context,
                                                                                    component,
                                                                                    clientName,
                                                                                    messagingServerName));
            }
        }

        private void updateClientStatus(ClientStatus clientStatus) {
            ClientIdentification clientIdentification =
                    new ClientIdentification(clientStatus.getContext(), clientStatus.getClientName());
            if (clientStatus.getConnected()) {
                if (! messagingServerName.equals(connectedClients.get(clientIdentification))) {
                    // Unknown client
                    eventPublisher.publishEvent(new TopologyEvents.ApplicationConnected(
                            clientStatus.getContext(),
                            clientStatus.getComponentName(),
                            clientStatus.getClientName(),
                            messagingServerName
                    ));
                } else {
                    logger.debug("Client {} connected to {} not forwarded, as already known to be connected to {}",
                                 clientIdentification.getClient(), messagingServerName, connectedClients.get(clientIdentification));
                }

            } else {
                if (messagingServerName.equals(connectedClients.get(clientIdentification))) {
                    commandHandlerPerContextClient.remove(clientIdentification);
                    queryHandlerPerContextClient.remove(clientIdentification);
                    eventPublisher.publishEvent(new TopologyEvents.ApplicationDisconnected(
                            clientStatus.getContext(),
                            clientStatus.getComponentName(),
                            clientStatus.getClientName(),
                            messagingServerName
                    ));
                } else {
                    logger.debug("Client {} disconnected from {} not forwarded, as already known to be connected to {}",
                                 clientIdentification.getClient(), messagingServerName, connectedClients.get(clientIdentification));
                }
            }
        }


        @Override
        public void onError(Throwable throwable) {
            logger.warn("{}: Error on connection from AxonServer node - {}", messagingServerName, throwable.getMessage());
            closeConnections();
        }

        @Override
        public void onCompleted() {
            closeConnections();
        }

        private void closeConnections() {
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
            if (messagingServerName != null) {
                connectedClients.entrySet().stream()
                                .filter(connectedClientEntry -> connectedClientEntry.getValue().equals(messagingServerName))
                                .forEach(connectedClientEntry -> eventPublisher.publishEvent(new TopologyEvents.ApplicationDisconnected(
                                        connectedClientEntry.getKey().getContext(), null, connectedClientEntry.getKey().getClient(), messagingServerName
                                )));
                clusterController.closeConnection(messagingServerName);
                eventPublisher.publishEvent(new ClusterEvents.AxonServerInstanceDisconnected(messagingServerName));
            }
        }
    }
}
