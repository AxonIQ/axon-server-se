package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonserver.EventProcessorEvents.ProcessorStatusRequest;
import io.axoniq.axonserver.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonserver.MetricsEvents;
import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.SubscriptionEvents;
import io.axoniq.axonserver.SubscriptionQueryEvents.SubscriptionQueryResponseReceived;
import io.axoniq.axonserver.TopologyEvents;
import io.axoniq.axonserver.TopologyEvents.CommandHandlerDisconnected;
import io.axoniq.axonserver.TopologyEvents.QueryHandlerDisconnected;
import io.axoniq.axonserver.UserSynchronizationEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.coordinator.RequestToBeCoordinatorReceived;
import io.axoniq.axonserver.enterprise.cluster.events.ApplicationSynchronizationEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.CoordinatorConfirmation;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.CoordinatorStepDown;
import io.axoniq.axonserver.enterprise.cluster.manager.RequestLeaderEvent;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.ReceivingStreamObserver;
import io.axoniq.axonserver.grpc.SendingStreamObserver;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.grpc.internal.Action;
import io.axoniq.axonserver.grpc.internal.Applications;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessor;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorSegment;
import io.axoniq.axonserver.grpc.internal.ClientStatus;
import io.axoniq.axonserver.grpc.internal.CommandHandlerStatus;
import io.axoniq.axonserver.grpc.internal.ConnectResponse;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand.RequestCase;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.Group;
import io.axoniq.axonserver.grpc.internal.MessagingClusterServiceGrpc;
import io.axoniq.axonserver.grpc.internal.ModelVersion;
import io.axoniq.axonserver.grpc.internal.NodeContext;
import io.axoniq.axonserver.grpc.internal.NodeContextInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.QueryHandlerStatus;
import io.axoniq.axonserver.grpc.internal.Users;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.application.ApplicationModelController;
import io.axoniq.platform.application.jpa.Application;
import io.axoniq.platform.user.User;
import io.axoniq.platform.user.UserController;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
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
 * On connect return the application db version number, so connecting server can see if it is up to date with defined applications.
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
    private final UserController userController;
    private final ApplicationController applicationController;
    private final ApplicationModelController applicationModelController;
    private final ContextController contextController;
    private final EventStoreLocator eventStoreManager;
    private final ApplicationEventPublisher eventPublisher;
    private final Map<String, ConnectorReceivingStreamObserver> connections = new ConcurrentHashMap<>();
    private final Map<RequestCase, Collection<BiConsumer<ConnectorCommand, Publisher<ConnectorResponse>>>> handlers
            = new EnumMap<>(RequestCase.class);


    @Value("${axoniq.axonserver.cluster.connectionCheckRetries:5}")
    private int connectionCheckRetries = 5;
    @Value("${axoniq.axonserver.cluster.connectionCheckRetryWait:1000}")
    private int connectionCheckRetryWait = 1000;
    @Value("${axoniq.axonserver.cluster.query-threads:1}")
    private final int queryProcessingThreads = 1;
    @Value("${axoniq.axonserver.cluster.command-threads:1}")
    private final int commandProcessingThreads = 1;
    private final Set<GrpcFlowControlledDispatcherListener> dispatchListeners = new CopyOnWriteArraySet<>();


    public MessagingClusterService(
            CommandDispatcher commandDispatcher,
            QueryDispatcher queryDispatcher,
            ClusterController clusterController,
            UserController userController,
            ApplicationController applicationController,
            ApplicationModelController applicationModelController,
            ContextController contextController,
            EventStoreLocator eventStoreManager,
            ApplicationEventPublisher eventPublisher) {
        this.commandDispatcher = commandDispatcher;
        this.queryDispatcher = queryDispatcher;
        this.clusterController = clusterController;
        this.userController = userController;
        this.applicationController = applicationController;
        this.applicationModelController = applicationModelController;
        this.contextController = contextController;
        this.eventStoreManager = eventStoreManager;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public StreamObserver<ConnectorCommand> openStream(StreamObserver<ConnectorResponse> responseObserver1) {
        SendingStreamObserver<ConnectorResponse> responseObserver = new SendingStreamObserver<>(responseObserver1);
        return new ConnectorReceivingStreamObserver(responseObserver);
    }

    @Override
    public void requestLeader(NodeContextInfo request, StreamObserver<Confirmation> responseObserver) {
        logger.debug("Received request leader {}", request);

        eventPublisher.publishEvent(new RequestLeaderEvent(request, result -> {
            try {
                logger.debug("Accept leader: {}", result);
                responseObserver.onNext(Confirmation.newBuilder().setSuccess(result).build());
                responseObserver.onCompleted();
            } catch (Exception ex) {
                logger.warn("Failed to publish event: {}", ex.getMessage());
            }
        }));
    }

    @PreDestroy
    public void shutdown() {
        dispatchListeners.forEach(GrpcFlowControlledDispatcherListener::cancel);
        dispatchListeners.clear();
    }

    @Override
    public void requestToBeCoordinator(NodeContext request, StreamObserver<Confirmation> responseObserver) {
        eventPublisher.publishEvent(new RequestToBeCoordinatorReceived(request, result -> {
            try {
                responseObserver.onNext(Confirmation.newBuilder().setSuccess(result).build());
                responseObserver.onCompleted();
            } catch (Exception ex) {
                logger.warn("Failed to publish event: {}", ex.getMessage());
            }
        }));
    }

    @EventListener
    public void on(ApplicationSynchronizationEvents.ApplicationReceived event) {
        if( event.isProxied()) return;

        connections.forEach((name, responseObserver) -> {
            try {
                responseObserver.publish(ConnectorResponse.newBuilder()
                                                         .setApplication(event.getApplication())
                                                         .build());
            } catch (Exception ex) {
                logger.debug("Error sending application to {} - {}", name, ex.getMessage());
            }
        });
    }

    @EventListener
    public void on(UserSynchronizationEvents.UserReceived event) {
        if( event.isProxied()) return;

        connections.forEach((name, responseObserver) -> {
            try {
                responseObserver.publish(ConnectorResponse.newBuilder()
                                                         .setUser(event.getUser())
                                                         .build());
            } catch (Exception ex) {
                logger.debug("Error sending application to {} - {}", name, ex.getMessage());
            }
        });
    }

    public void sendToAll(ConnectorResponse response, Function<String, String> errorMessage){
        connections.forEach((name, responseObserver) -> {
            try {
                responseObserver.publish(response);
            } catch (Exception ex) {
                logger.debug("{} - {}", errorMessage.apply(name), ex.getMessage());
            }
        });
    }

    public void onConnectorCommand(RequestCase requestCase, BiConsumer<ConnectorCommand, Publisher<ConnectorResponse>> consumer){
        this.handlers.computeIfAbsent(requestCase, (rc) -> new CopyOnWriteArraySet<>()).add(consumer);
    }



    @Override
    public void join(NodeInfo request, StreamObserver<NodeInfo> responseObserver) {
        try {
            checkConnection(request.getInternalHostName());
            checkMasterForAllStorageContexts(request.getContextsList());
            clusterController.addConnection(request, true);
            clusterController.nodes().forEach(clusterNode -> responseObserver
                    .onNext(clusterNode.toNodeInfo()));
            responseObserver.onCompleted();
        } catch (Exception mpe) {
            logger.warn("Join request failed", mpe);
            responseObserver.onError(GrpcExceptionBuilder.build(mpe));
        }
    }

    private void checkMasterForAllStorageContexts(List<ContextRole> contextsList) {
        for (ContextRole contextRole : contextsList) {
            if( contextRole.getStorage() && eventStoreManager.getMaster(contextRole.getName()) == null && clusterController.disconnectedNodes()) {
                throw new MessagingPlatformException(ErrorCode.CANNOT_JOIN, "Cannot join context " + contextRole.getName()
                        + " for storage as it does not have an active master and not all AxonServer nodes are connected");
            }
        }
    }

    private void checkConnection(String internalHostName)  {
        int retries  = connectionCheckRetries;
        while( retries-- > 0) {
            try {
                InetAddress.getAllByName(internalHostName);
                return;
            } catch (UnknownHostException unknownHost) {
                if (retries == 0)
                    throw new MessagingPlatformException(ErrorCode.UNKNOWN_HOST, "Unknown host: " + internalHostName);
                try {
                    logger.warn("Failed to resolve hostname {}, retrying in one second", internalHostName);
                    Thread.sleep(connectionCheckRetryWait);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new MessagingPlatformException(ErrorCode.UNKNOWN_HOST, "Unknown host: " + internalHostName);
                }
            }
        }
    }

    private class ConnectorReceivingStreamObserver extends ReceivingStreamObserver<ConnectorCommand> {

        private final CopyOnWriteArraySet<String> clients;
        private final SendingStreamObserver<ConnectorResponse> responseObserver;
        private volatile GrpcInternalCommandDispatcherListener commandQueueListener;
        private volatile GrpcInternalQueryDispatcherListener queryQueueListener;
        private volatile String messagingServerName;

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
                            messagingServerName = connectorCommand.getConnect().getNodeName();
                            clusterController.addConnection(connectorCommand.getConnect(), false);
                            logger.debug("Received connect from: {} - {}",
                                         messagingServerName,
                                         connectorCommand.getConnect());

                            ConnectResponse.Builder connectResponseBuilder = ConnectResponse.newBuilder()
                                                                                            .addAllModelVersions(
                                                                                                    applicationModelController
                                                                                                            .getModelVersions()
                                                                                                            .stream()
                                                                                                            .map(m -> ModelVersion
                                                                                                                    .newBuilder()
                                                                                                                    .setName(
                                                                                                                            m.getApplicationName())
                                                                                                                    .setValue(
                                                                                                                            m.getVersion())
                                                                                                                    .build())
                                                                                                            .collect(
                                                                                                                    Collectors
                                                                                                                            .toList())
                                                                                            )
                                                                                            .addAllContexts(
                                                                                                    clusterController
                                                                                                            .getMyContexts()
                                                                                                            .stream()
                                                                                                            .map(c -> ContextRole
                                                                                                                    .newBuilder()
                                                                                                                    .setName(
                                                                                                                            c.getContext()
                                                                                                                             .getName())
                                                                                                                    .setMessaging(
                                                                                                                            c.isMessaging())
                                                                                                                    .setStorage(
                                                                                                                            c.isStorage())
                                                                                                                    .build()
                                                                                                            ).collect(
                                                                                                            Collectors
                                                                                                                    .toList()));

                            clusterController.nodes()
                                             .filter(c -> !c.getName().equals(messagingServerName))
                                             .forEach(c -> connectResponseBuilder.addNodes(c.toNodeInfo()));
                            responseObserver.onNext(ConnectorResponse.newBuilder()
                                                                     .setConnectResponse(connectResponseBuilder)
                                                                     .build());
                            connections.put(messagingServerName, this);
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
                        eventPublisher.publishEvent(new SubscriptionEvents.SubscribeCommand(connectorCommand
                                                                                                    .getSubscribeCommand()
                                                                                                    .getContext(),
                                                                                            command,
                                                                                            new ProxyCommandHandler(
                                                                                                    responseObserver,
                                                                                                    command.getClientId(),
                                                                                                    command.getComponentName(),
                                                                                                    messagingServerName)
                        ));
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
                        commandDispatcher.handleResponse(connectorCommand.getCommandResponse(),true);
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

                        eventPublisher.publishEvent(new SubscriptionEvents.SubscribeQuery(connectorCommand
                                                                                                  .getSubscribeQuery()
                                                                                                  .getContext(),
                                                                                          query
                                , new ProxyQueryHandler(responseObserver,
                                                        query.getClientId(),
                                                        query.getComponentName(),
                                                        messagingServerName)
                        ));

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
                        queryDispatcher.handleComplete(connectorCommand.getQueryComplete().getMessageId(),
                                                       connectorCommand.getQueryComplete().getClient(),
                                                       true);
                        break;
                    case FLOW_CONTROL:
                        logger.debug("FLOW_CONTROL {}", connectorCommand.getFlowControl());
                        handleFlowControl(connectorCommand);
                        break;
                    case DELETE_NODE:
                        clusterController.deleteNode(connectorCommand.getDeleteNode().getNodeName());
                        break;
                    case REQUEST_APPLICATIONS:
                        Applications.Builder applicationsBuilder = Applications.newBuilder()
                                                                               .setVersion(applicationModelController
                                                                                                   .getModelVersion(
                                                                                                           Application.class));
                        applicationController.getApplications().forEach(app ->
                                                                                applicationsBuilder.addApplication(
                                                                                        ProtoConverter
                                                                                                .createApplication(
                                                                                                        app,
                                                                                                        Action.MERGE)));
                        responseObserver.onNext(ConnectorResponse.newBuilder().setApplications(applicationsBuilder)
                                                                 .build());
                        break;
                    case REQUEST_USERS:
                        Users.Builder usersBuilder = Users.newBuilder().setVersion(applicationModelController
                                                                                           .getModelVersion(User.class));
                        userController.getUsers().forEach(user ->
                                                                  usersBuilder.addUser(ProtoConverter.createUser(
                                                                          user,
                                                                          Action.MERGE))
                        );
                        responseObserver.onNext(ConnectorResponse.newBuilder().setUsers(usersBuilder).build());
                        break;
                    case DB_STATUS:
                        break;
                    case CONTEXT:
                        contextController.update(connectorCommand.getContext()).forEach(eventPublisher::publishEvent);
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
                            eventPublisher.publishEvent(new QueryHandlerDisconnected(queryHandlerStatus.getContext(), queryHandlerStatus.getClientName(), true));
                        }
                        break;
                    case COMMAND_HANDLER_STATUS:
                        CommandHandlerStatus commandHandlerStatus = connectorCommand.getCommandHandlerStatus();
                        if (!commandHandlerStatus.getConnected()){
                            eventPublisher.publishEvent(new CommandHandlerDisconnected(commandHandlerStatus.getContext(), commandHandlerStatus.getClientName(), true));
                        }
                        break;
                    case CLIENT_EVENT_PROCESSOR_STATUS:
                        eventPublisher.publishEvent(
                                new EventProcessorStatusUpdate(connectorCommand.getClientEventProcessorStatus(),
                                                               true));
                        break;
                    case START_CLIENT_EVENT_PROCESSOR:
                        ClientEventProcessor startProcessor = connectorCommand.getStartClientEventProcessor();
                        eventPublisher.publishEvent(
                                new StartEventProcessorRequest(startProcessor.getClient(),
                                                               startProcessor.getProcessorName(), true));
                        break;
                    case PAUSE_CLIENT_EVENT_PROCESSOR:
                        ClientEventProcessor pauseProcessor = connectorCommand.getPauseClientEventProcessor();
                        eventPublisher.publishEvent(
                                new PauseEventProcessorRequest(pauseProcessor.getClient(),
                                                               pauseProcessor.getProcessorName(), true));
                        break;
                    case COORDINATOR_CONFIRMATION:
                        NodeContext msg = connectorCommand.getCoordinatorConfirmation();
                        Object event = (msg.getNodeName().isEmpty()) ?
                                new CoordinatorStepDown(msg.getContext(), true) :
                                new CoordinatorConfirmation(msg.getNodeName(), msg.getContext(), true);
                        eventPublisher.publishEvent(event);
                        break;
                    case RELEASE_SEGMENT:
                        ClientEventProcessorSegment releaseSegment = connectorCommand.getReleaseSegment();
                        eventPublisher.publishEvent(new ReleaseSegmentRequest(releaseSegment.getClient(),
                                                                              releaseSegment.getProcessorName(),
                                                                              releaseSegment.getSegmentIdentifier(),
                                                                              true));
                        break;
                    case REQUEST_PROCESSOR_STATUS:
                        ClientEventProcessor requestStatus = connectorCommand.getRequestProcessorStatus();
                        eventPublisher.publishEvent(new ProcessorStatusRequest(requestStatus.getClient(),
                                                                               requestStatus.getProcessorName(),
                                                                               true));
                        break;
                    case SUBSCRIPTION_QUERY_RESPONSE:
                            SubscriptionQueryResponse response = connectorCommand.getSubscriptionQueryResponse();
                            eventPublisher.publishEvent(new SubscriptionQueryResponseReceived(response));
                            break;
                    case MASTER_CONFIRMATION:
                        logger.info("{}: Received master confirmation {}", messagingServerName, connectorCommand.getMasterConfirmation());
                        if(StringUtils.isBlank(connectorCommand.getMasterConfirmation().getNodeName())) {
                            eventPublisher.publishEvent(new ClusterEvents.MasterStepDown(connectorCommand.getMasterConfirmation()
                                                                                                         .getContext(),
                                                                                         true));

                        } else {
                            eventPublisher.publishEvent(new ClusterEvents.MasterConfirmation(connectorCommand.getMasterConfirmation()
                                                                                                             .getContext(),
                                                                                             connectorCommand.getMasterConfirmation()
                                                                                               .getNodeName(), true));
                        }
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
            if( clients.add(clientName)) {
                eventPublisher.publishEvent(new TopologyEvents.ApplicationConnected(context,
                                                                                    component,
                                                                                    clientName,
                                                                                    messagingServerName));
            }
        }

        private void updateClientStatus(ClientStatus clientStatus) {
            if( clientStatus.getConnected()) {

                if( clients.add(clientStatus.getClientName())) {
                    // unknown client
                    eventPublisher.publishEvent(new TopologyEvents.ApplicationConnected(clientStatus.getContext(),
                                                                                       clientStatus.getComponentName(),
                                                                                       clientStatus.getClientName(),
                                                                                       messagingServerName));
                }
            } else {
                if( clients.remove(clientStatus.getClientName())) {
                    // known client
                    logger.info("Client disconnected: {}", clientStatus.getClientName());
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
            clients.forEach(client -> eventPublisher.publishEvent(new TopologyEvents.ApplicationDisconnected(null,
                                                                                                        null,
                                                                                                        client,
                                                                                                        messagingServerName)));
            eventPublisher.publishEvent(new ClusterEvents.AxonServerInstanceDisconnected(messagingServerName));
        }

        public void publish(ConnectorResponse connectorResponse) {
            responseObserver.onNext(connectorResponse);
        }
    }
}
