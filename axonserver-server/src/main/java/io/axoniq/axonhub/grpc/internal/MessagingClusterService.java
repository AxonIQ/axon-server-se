package io.axoniq.axonhub.grpc.internal;

import io.axoniq.axonhub.ApplicationSynchronizationEvents;
import io.axoniq.axonhub.ClusterEvents;
import io.axoniq.axonhub.ClusterEvents.CoordinatorConfirmation;
import io.axoniq.axonhub.ClusterEvents.CoordinatorStepDown;
import io.axoniq.axonhub.CommandSubscription;
import io.axoniq.axonhub.Confirmation;
import io.axoniq.axonhub.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonhub.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonhub.EventProcessorEvents.ProcessorStatusRequest;
import io.axoniq.axonhub.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonhub.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonhub.MetricsEvents;
import io.axoniq.axonhub.ProcessingInstructionHelper;
import io.axoniq.axonhub.QuerySubscription;
import io.axoniq.axonhub.SubscriptionEvents;
import io.axoniq.axonhub.SubscriptionQueryEvents.SubscriptionQueryResponseReceived;
import io.axoniq.axonhub.SubscriptionQueryResponse;
import io.axoniq.axonhub.UserSynchronizationEvents;
import io.axoniq.axonhub.cluster.ClusterController;
import io.axoniq.axonhub.cluster.coordinator.RequestToBeCoordinatorReceived;
import io.axoniq.axonhub.context.ContextController;
import io.axoniq.axonhub.exception.ErrorCode;
import io.axoniq.axonhub.exception.MessagingPlatformException;
import io.axoniq.axonhub.grpc.GrpcExceptionBuilder;
import io.axoniq.axonhub.grpc.ProtoConverter;
import io.axoniq.axonhub.grpc.Publisher;
import io.axoniq.axonhub.grpc.ReceivingStreamObserver;
import io.axoniq.axonhub.grpc.SendingStreamObserver;
import io.axoniq.axonhub.internal.grpc.Applications;
import io.axoniq.axonhub.internal.grpc.ClientEventProcessor;
import io.axoniq.axonhub.internal.grpc.ClientEventProcessorSegment;
import io.axoniq.axonhub.internal.grpc.ClientStatus;
import io.axoniq.axonhub.internal.grpc.ConnectResponse;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand.RequestCase;
import io.axoniq.axonhub.internal.grpc.ConnectorResponse;
import io.axoniq.axonhub.internal.grpc.ContextRole;
import io.axoniq.axonhub.internal.grpc.Group;
import io.axoniq.axonhub.internal.grpc.MessagingClusterServiceGrpc;
import io.axoniq.axonhub.internal.grpc.NodeContext;
import io.axoniq.axonhub.internal.grpc.NodeContextInfo;
import io.axoniq.axonhub.internal.grpc.NodeInfo;
import io.axoniq.axonhub.internal.grpc.Users;
import io.axoniq.axonhub.message.command.CommandDispatcher;
import io.axoniq.axonhub.message.event.RequestLeaderEvent;
import io.axoniq.axonhub.message.query.QueryDispatcher;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.grpc.Action;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Handles requests from other axonhub cluster servers acting as message processors.
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
 * Author: marc
 */
@Service("MessagingClusterService")
public class MessagingClusterService extends MessagingClusterServiceGrpc.MessagingClusterServiceImplBase {
    private final Logger logger = LoggerFactory.getLogger(MessagingClusterService.class);
    private final CommandDispatcher commandDispatcher;
    private final QueryDispatcher queryDispatcher;
    private final ClusterController clusterController;
    private final UserController userController;
    private final ApplicationController applicationController;
    private final ContextController contextController;
    private final ApplicationEventPublisher eventPublisher;
    private final Map<String, ConnectorReceivingStreamObserver> connections = new ConcurrentHashMap<>();
    private final Map<RequestCase, Collection<BiConsumer<ConnectorCommand, Publisher<ConnectorResponse>>>> handlers
            = new EnumMap<>(RequestCase.class);


    @Value("${axoniq.axonhub.cluster.connectionCheckRetries:5}")
    private int connectionCheckRetries = 5;
    @Value("${axoniq.axonhub.cluster.connectionCheckRetryWait:1000}")
    private int connectionCheckRetryWait = 1000;

    public MessagingClusterService(
            CommandDispatcher commandDispatcher,
            QueryDispatcher queryDispatcher,
            ClusterController clusterController,
            UserController userController,
            ApplicationController applicationController,
            ContextController contextController,
            ApplicationEventPublisher eventPublisher) {
        this.commandDispatcher = commandDispatcher;
        this.queryDispatcher = queryDispatcher;
        this.clusterController = clusterController;
        this.userController = userController;
        this.applicationController = applicationController;
        this.contextController = contextController;
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
            clusterController.addConnection(request);
            clusterController.messagingNodes().forEach(clusterNode -> responseObserver
                    .onNext(clusterNode.toNodeInfo()));
            responseObserver.onCompleted();
        } catch (Exception mpe) {
            logger.warn("Join request failed", mpe);
            responseObserver.onError(GrpcExceptionBuilder.build(mpe));
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
                        messagingServerName = connectorCommand.getConnect().getNodeName();
                        clusterController.addConnection(connectorCommand.getConnect());
                        logger.debug("Received connect from: {} - {}", messagingServerName, connectorCommand.getConnect());

                        ConnectResponse.Builder connectResponseBuilder = ConnectResponse.newBuilder()
                                .setApplicationModelVersion(applicationController.getModelVersion())
                                .addAllContexts(clusterController.getMyContexts().stream().map(c -> ContextRole.newBuilder()
                                                                                                               .setName(c.getContext().getName())
                                                                                               .setMessaging(c.isMessaging())
                                                                                               .setStorage(c.isStorage())
                                                                                               .build()
                                                ).collect(Collectors.toList()));

                        clusterController.getRemoteConnections().stream()
                                         .filter(c -> ! c.getClusterNode().getName().equals(messagingServerName))
                                         .forEach(c -> connectResponseBuilder.addNodes(c.getClusterNode().toNodeInfo()));
                        responseObserver.onNext(ConnectorResponse.newBuilder()
                                                                 .setConnectResponse(connectResponseBuilder)
                                                                 .build());
                        connections.put(messagingServerName, this);
                        break;
                    case SUBSCRIBE_COMMAND:
                        CommandSubscription command = connectorCommand.getSubscribeCommand()
                                                                      .getCommand();
                        logger.debug("SUBSCRIBE [{}] [{}] [{}]", command.getCommand(),
                                     command.getClientName(),
                                     messagingServerName);

                        checkClient(connectorCommand.getSubscribeCommand().getContext(),
                                    command.getComponentName(),
                                    command.getClientName());
                        eventPublisher.publishEvent(new SubscriptionEvents.SubscribeCommand(connectorCommand
                                                                                                    .getSubscribeCommand()
                                                                                                    .getContext(),
                                                                                            command,
                                                                                            new ProxyCommandHandler(
                                                                                                    responseObserver,
                                                                                                    command.getClientName(),
                                                                                                    command.getComponentName(),
                                                                                                    messagingServerName)
                        ));
                        break;
                    case UNSUBSCRIBE_COMMAND:
                        logger.debug("UNSUBSCRIBE [{}] [{}] [{}]",
                                     connectorCommand.getUnsubscribeCommand().getCommand(),
                                     connectorCommand.getUnsubscribeCommand().getCommand().getClientName(),
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
                                     query.getClientName(),
                                     messagingServerName);
                        checkClient(connectorCommand.getSubscribeQuery().getContext(),
                                    query.getComponentName(),
                                    query.getClientName());

                        eventPublisher.publishEvent(new SubscriptionEvents.SubscribeQuery(connectorCommand
                                                                                                  .getSubscribeQuery()
                                                                                                  .getContext(),
                                                                                          query
                                , new ProxyQueryHandler(responseObserver,
                                                        query.getClientName(),
                                                        query.getComponentName(),
                                                        messagingServerName)
                        ));

                        break;
                    case UNSUBSCRIBE_QUERY:
                        logger.debug("UNSUBSCRIBE [{}/{}] [{}] [{}]",
                                     connectorCommand.getUnsubscribeQuery().getQuery().getQuery(),
                                     connectorCommand.getUnsubscribeQuery().getQuery().getResultName(),
                                     connectorCommand.getUnsubscribeQuery().getQuery().getClientName(),
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
                                                                               .setVersion(applicationController
                                                                                                   .getModelVersion());
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
                        Users.Builder usersBuilder = Users.newBuilder().setVersion(applicationController
                                                                                           .getModelVersion());
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
                                                                                     responseObserver);
                }
                commandQueueListener.addPermits(connectorCommand.getFlowControl().getPermits());
            }
            if (Group.QUERY.equals(connectorCommand.getFlowControl().getGroup())) {
                if (queryQueueListener == null) {
                    queryQueueListener = new GrpcInternalQueryDispatcherListener(queryDispatcher,
                                                                                 connectorCommand
                                                                                         .getFlowControl()
                                                                                         .getNodeName(),
                                                                                 responseObserver);
                }
                queryQueueListener.addPermits(connectorCommand.getFlowControl().getPermits());
            }
        }

        @Override
        protected String sender() {
            return messagingServerName;
        }

        private void checkClient(String context, String component, String clientName) {
            if( clients.add(clientName)) {
                eventPublisher.publishEvent(new ClusterEvents.ApplicationConnected(context,
                                                                                   component,
                                                                                   clientName,
                                                                                   messagingServerName));
            }
        }

        private void updateClientStatus(ClientStatus clientStatus) {
            if( clientStatus.getConnected()) {

                if( clients.add(clientStatus.getClientName())) {
                    // unknown client
                    eventPublisher.publishEvent(new ClusterEvents.ApplicationConnected(clientStatus.getContext(),
                                                                                       clientStatus.getComponentName(),
                                                                                       clientStatus.getClientName(),
                                                                                       messagingServerName));
                }
            } else {
                if( clients.remove(clientStatus.getClientName())) {
                    // known client
                    logger.info("Client disconnected: {}", clientStatus.getClientName());
                    eventPublisher.publishEvent(new ClusterEvents.ApplicationDisconnected(clientStatus.getContext(),
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
                commandQueueListener = null;
            }
            if (queryQueueListener != null) {
                queryQueueListener.cancel();
                queryQueueListener = null;
            }
            clients.forEach(client -> eventPublisher.publishEvent(new ClusterEvents.ApplicationDisconnected(null,
                                                                                                        null,
                                                                                                        client,
                                                                                                        messagingServerName)));
            eventPublisher.publishEvent(new ClusterEvents.AxonHubInstanceDisconnected(messagingServerName));
        }

        public void publish(ConnectorResponse connectorResponse) {
            responseObserver.onNext(connectorResponse);
        }
    }
}
