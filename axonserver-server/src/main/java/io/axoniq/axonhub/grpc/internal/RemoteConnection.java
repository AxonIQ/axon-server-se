package io.axoniq.axonhub.grpc.internal;

import io.axoniq.axonhub.ApplicationSynchronizationEvents;
import io.axoniq.axonhub.ClusterEvents;
import io.axoniq.axonhub.CommandSubscription;
import io.axoniq.axonhub.DispatchEvents;
import io.axoniq.axonhub.LoadBalancingSynchronizationEvents.LoadBalancingStrategiesReceived;
import io.axoniq.axonhub.LoadBalancingSynchronizationEvents.LoadBalancingStrategyReceived;
import io.axoniq.axonhub.LoadBalancingSynchronizationEvents.ProcessorLoadBalancingStrategyReceived;
import io.axoniq.axonhub.LoadBalancingSynchronizationEvents.ProcessorsLoadBalanceStrategyReceived;
import io.axoniq.axonhub.ProcessingInstruction;
import io.axoniq.axonhub.ProcessingInstructionHelper;
import io.axoniq.axonhub.ProcessingKey;
import io.axoniq.axonhub.QueryRequest;
import io.axoniq.axonhub.QueryResponse;
import io.axoniq.axonhub.QuerySubscription;
import io.axoniq.axonhub.SubscriptionQueryEvents.ProxiedSubscriptionQueryRequest;
import io.axoniq.axonhub.UserSynchronizationEvents;
import io.axoniq.axonhub.cluster.ClusterController;
import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import io.axoniq.axonhub.grpc.ClusterFlowControlStreamObserver;
import io.axoniq.axonhub.grpc.ManagedChannelHelper;
import io.axoniq.axonhub.grpc.ReceivingStreamObserver;
import io.axoniq.axonhub.grpc.StubFactory;
import io.axoniq.axonhub.internal.grpc.ClientStatus;
import io.axoniq.axonhub.internal.grpc.ClientSubscriptionQueryRequest;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.ConnectorResponse;
import io.axoniq.axonhub.internal.grpc.ContextRole;
import io.axoniq.axonhub.internal.grpc.GetApplicationsRequest;
import io.axoniq.axonhub.internal.grpc.GetUsersRequest;
import io.axoniq.axonhub.internal.grpc.InternalCommandSubscription;
import io.axoniq.axonhub.internal.grpc.InternalQuerySubscription;
import io.axoniq.axonhub.internal.grpc.NodeInfo;
import io.axoniq.axonhub.internal.grpc.QueryComplete;
import io.axoniq.axonhub.message.query.QueryDefinition;
import io.axoniq.axonhub.message.query.subscription.UpdateHandler;
import io.axoniq.axonhub.message.query.subscription.handler.ProxyUpdateHandler;
import io.axoniq.platform.MetaDataValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Holds connection to other axonhub platform node. Receives commands and queries from the other node to execute.
 * Each subscription made to this node is forwarded to the connected node.
 * <p>
 * Managed by {@link ClusterController}, which will check connection status and try to reconnect lost connections.
 * Author: marc
 */
public class RemoteConnection  {
    private static final Logger logger = LoggerFactory.getLogger(RemoteConnection.class);
    private final ClusterNode me;
    private final ClusterNode clusterNode;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final StubFactory stubFactory;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private volatile boolean connected = false;
    private volatile long connectionPending;
    private volatile ClusterFlowControlStreamObserver requestStreamObserver;
    private volatile String errorMessage;
    private final long connectionWaitTime;


    public RemoteConnection(ClusterNode me, ClusterNode clusterNode,
                            ApplicationEventPublisher applicationEventPublisher,
                            StubFactory stubFactory,
                            MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.me = me;
        this.clusterNode = clusterNode;
        this.applicationEventPublisher = applicationEventPublisher;
        this.stubFactory = stubFactory;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.connectionWaitTime = messagingPlatformConfiguration.getCluster().getConnectionWaitTime();
    }

    public synchronized RemoteConnection init() {
        logger.debug("Connecting to: {}:{}", clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort());
        try {
            InetAddress[] addresses = InetAddress.getAllByName(clusterNode.getInternalHostName());
            logger.debug("Connect to {}", addresses);
        } catch (UnknownHostException e) {
            logger.warn("Unknown host: {}", clusterNode.getInternalHostName());
            return this;
        }
        requestStreamObserver = new ClusterFlowControlStreamObserver(stubFactory.messagingClusterServiceStub(messagingPlatformConfiguration, clusterNode)
                .openStream(new ReceivingStreamObserver<ConnectorResponse>(logger) {
                    @Override
                    protected void consume(ConnectorResponse connectorResponse) {
                        if (!connected) {
                            logger.debug("Connected to {}:{}", clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort());
                            connected = true;
                            errorMessage = null;
                            connectionPending = 0;
                            initFlowControl();
                        }

                            switch (connectorResponse.getResponseCase()) {
                                case CONFIRMATION:
                                    break;

                                case COMMAND:
                                    applicationEventPublisher.publishEvent(
                                            new DispatchEvents.DispatchCommand(ProcessingInstructionHelper
                                                                                       .context(connectorResponse
                                                                                                        .getCommand()
                                                                                                        .getProcessingInstructionsList()),
                                                                               connectorResponse.getCommand(),
                                                                               commandResponse -> publish(
                                                                                       ConnectorCommand.newBuilder()
                                                                                                       .setCommandResponse(
                                                                                                               commandResponse)
                                                                                                       .build()),
                                                                               true));
                                    break;

                                case QUERY:
                                    applicationEventPublisher.publishEvent(
                                            new DispatchEvents.DispatchQuery(ProcessingInstructionHelper
                                                                                     .context(connectorResponse
                                                                                                      .getQuery()
                                                                                                      .getProcessingInstructionsList()),
                                                                             connectorResponse.getQuery(),
                                                                             queryResponse -> sendQueryResponse(
                                                                                     connectorResponse.getQuery(),
                                                                                     queryResponse),
                                                                             client -> sendQueryComplete(
                                                                                     connectorResponse.getQuery(),
                                                                                     client),
                                                                             true));
                                    break;

                                case CONNECT_RESPONSE:
                                    logger.debug("Connected, received response: {}",
                                                 connectorResponse.getConnectResponse());
                                    try {


                                        applicationEventPublisher
                                                .publishEvent(new ClusterEvents.AxonHubInstanceConnected(
                                                        RemoteConnection.this,
                                                        connectorResponse.getConnectResponse()
                                                                         .getApplicationModelVersion(),
                                                        connectorResponse.getConnectResponse().getContextsList(),
                                                        connectorResponse.getConnectResponse().getNodesList()));
                                    } catch (Exception ex) {
                                        logger.warn("Failed to process request {}",
                                                    connectorResponse.getConnectResponse(),
                                                    ex);
                                    }
                                    break;

                                case APPLICATION:
                                    applicationEventPublisher
                                            .publishEvent(new ApplicationSynchronizationEvents.ApplicationReceived(
                                                    connectorResponse.getApplication(),
                                                    true));
                                    break;

                                case USER:
                                    applicationEventPublisher.publishEvent(new UserSynchronizationEvents.UserReceived(
                                            connectorResponse.getUser(), true));
                                    break;

                                case APPLICATIONS:
                                    applicationEventPublisher
                                            .publishEvent(new ApplicationSynchronizationEvents.ApplicationsReceived(
                                                    connectorResponse.getApplications()));
                                    break;

                                case USERS:
                                    applicationEventPublisher.publishEvent(new UserSynchronizationEvents.UsersReceived(
                                            connectorResponse.getUsers()));
                                    break;
                                case PROCESSOR_STRATEGY:
                                    applicationEventPublisher.publishEvent(new ProcessorLoadBalancingStrategyReceived(
                                      connectorResponse.getProcessorStrategy(), true));
                                    break;
                                case PROCESSORS_STRATEGIES:
                                    applicationEventPublisher.publishEvent(new ProcessorsLoadBalanceStrategyReceived(
                                            connectorResponse.getProcessorsStrategies()));
                                    break;
                                case LOAD_BALANCING_STRATEGY:
                                    applicationEventPublisher.publishEvent(new LoadBalancingStrategyReceived(
                                            connectorResponse.getLoadBalancingStrategy(), true));
                                    break;
                                case LOAD_BALANCING_STRATEGIES:
                                    applicationEventPublisher.publishEvent(new LoadBalancingStrategiesReceived(
                                            connectorResponse.getLoadBalancingStrategies()));
                                    break;
                                case SUBSCRIPTION_QUERY_REQUEST:
                                    ClientSubscriptionQueryRequest request = connectorResponse.getSubscriptionQueryRequest();
                                    UpdateHandler handler = new ProxyUpdateHandler(requestStreamObserver::onNext);
                                    applicationEventPublisher.publishEvent(
                                            new ProxiedSubscriptionQueryRequest(request.getSubscriptionQueryRequest(),
                                                                                handler,
                                                                                request.getClient()));
                                    break;
                                default:
                                    break;
                            }
                    }

                    @Override
                    protected String sender() {
                        return clusterNode.getName();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        if (!String.valueOf(throwable.getMessage()).equals(errorMessage)) {
                            ManagedChannelHelper.checkShutdownNeeded(clusterNode.getName(), throwable);
                            logger.warn("Error on {}:{} - {}", clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort(), throwable.getMessage());
                            errorMessage = String.valueOf(throwable.getMessage());
                        }
                        closeConnection();
                        logger.debug("Connected: {}, connection pending: {}", connected, connectionPending);
                    }

                    private void closeConnection() {
                        if( connected) {
                            applicationEventPublisher.publishEvent(new ClusterEvents.AxonHubInstanceDisconnected(clusterNode.getName()));
                        }
                        connected = false;
                        connectionPending = 0;
                    }

                    @Override
                    public void onCompleted() {
                        logger.debug("Completed connection to {}:{}", clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort());
                        closeConnection();
                    }
                }));
        requestStreamObserver.onNext(ConnectorCommand.newBuilder()
                .setConnect(NodeInfo.newBuilder()
                        .setNodeName(messagingPlatformConfiguration.getName())
                        .setGrpcInternalPort(messagingPlatformConfiguration.getInternalPort())
                        .setGrpcPort(messagingPlatformConfiguration.getPort())
                        .setHttpPort(messagingPlatformConfiguration.getHttpPort())
                        .setVersion(1)
                        .setHostName(messagingPlatformConfiguration.getFullyQualifiedHostname())
                        .setInternalHostName(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())
                                    .addAllContexts(me.getContexts().stream().map(context ->
                                                                                 ContextRole.newBuilder()
                                                                                            .setName(context.getContext().getName())
                                                                                            .setMessaging(context.isMessaging())
                                                                                            .setStorage(context.isStorage())
                                                                                            .build()).collect(Collectors.toList()))
                        .build())
                .build());

        // send master info
        connectionPending = System.currentTimeMillis();
        return this;
    }

    private void initFlowControl() {
        requestStreamObserver.initCommandFlowControl(messagingPlatformConfiguration);
        requestStreamObserver.initQueryFlowControl(messagingPlatformConfiguration);

    }

    private void sendQueryResponse( QueryRequest query, QueryResponse queryResponse) {
        requestStreamObserver.onNext(ConnectorCommand.newBuilder().setQueryResponse(
                QueryResponse.newBuilder(queryResponse)
                        .addProcessingInstructions(ProcessingInstruction.newBuilder()
                                .setKey(ProcessingKey.TARGET_CLIENT)
                                .setValue(MetaDataValue.newBuilder().setTextValue(ProcessingInstructionHelper.targetClient(query.getProcessingInstructionsList())))
                        )
        ).build());
    }

    private void sendQueryComplete( QueryRequest query, String client) {
        requestStreamObserver.onNext(ConnectorCommand.newBuilder()
                                                     .setQueryComplete(
                                                             QueryComplete.newBuilder()
                                                                          .setMessageId(query.getMessageIdentifier())
                                                                          .setClient(client))
                                                     .build());
    }

    public void close() {
        if (connected) {
            if( requestStreamObserver != null) requestStreamObserver.onCompleted();
            connected = false;
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public void checkConnection() {
        if( logger.isDebugEnabled() && System.currentTimeMillis() - connectionPending < connectionWaitTime)
            logger.debug("Connection pending to: {}:{}", clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort());
        if (!connected && System.currentTimeMillis() - connectionPending > connectionWaitTime) {
            init();
        }
    }

    public void unsubscribeCommand(String context, String command, String client, String componentName) {
            publish(ConnectorCommand.newBuilder()
                    .setUnsubscribeCommand(InternalCommandSubscription.newBuilder()
                            .setCommand(
                                    CommandSubscription.newBuilder()
                                            .setClientName(client)
                                            .setCommand(command)
                                            .setComponentName(componentName)
                            ).setContext(context))
                    .build());

    }

    public void subscribeCommand(String context, String command, String client, String componentName) {
            publish(ConnectorCommand.newBuilder()
                    .setSubscribeCommand(InternalCommandSubscription.newBuilder()
                            .setCommand(CommandSubscription.newBuilder()
                                    .setClientName(client)
                                    .setCommand(command)
                                    .setComponentName(componentName)
                            ).setContext(context))
                    .build());
    }

    public ClusterNode getClusterNode() {
        return clusterNode;
    }

    public void subscribeQuery(QueryDefinition query, Collection<String> resultNames, String component, String clientName) {
            resultNames.forEach(resultName->
                publish(
                        ConnectorCommand.newBuilder().setSubscribeQuery(
                                InternalQuerySubscription.newBuilder().setQuery(
                                        QuerySubscription.newBuilder()
                                                         .setClientName(clientName)
                                                         .setQuery(query.getQueryName())
                                                         .setResultName(resultName)
                                                         .setComponentName(component)
                                ).setContext(query.getContext())
                        ).build()));
    }

    public void unsubscribeQuery(QueryDefinition queryDefinition, String componentName, String client) {
        publish(ConnectorCommand.newBuilder()
                    .setUnsubscribeQuery(InternalQuerySubscription.newBuilder()
                            .setQuery(QuerySubscription.newBuilder()
                                    .setClientName(client)
                                    .setQuery(queryDefinition.getQueryName())
                                    .setComponentName(componentName))
                            .setContext(queryDefinition.getContext())
                    ).build());
    }

    public void requestApplications() {
        publish(ConnectorCommand.newBuilder()
                    .setRequestApplications(GetApplicationsRequest.newBuilder())
                    .build());
    }

    public void sendDelete(String name) {
        publish(ConnectorCommand.newBuilder()
                    .setDeleteNode(NodeInfo.newBuilder().setNodeName(name))
                    .build());
    }

    public void requestUsers() {
        publish(ConnectorCommand.newBuilder()
                                                         .setRequestUsers(GetUsersRequest.newBuilder().build())
                                                         .build());
    }

    public void clientStatus(String context, String componentName, String client, boolean clientConnected) {
        publish(ConnectorCommand.newBuilder()
                                                         .setClientStatus(ClientStatus
                                                                                  .newBuilder()
                                                                                  .setContext(context)
                                                                                  .setComponentName(componentName)
                                                                                  .setClientName(client)
                                                                          .setConnected(clientConnected)
                                                         )
                                                         .build());
    }

    public void publish(ConnectorCommand connectorCommand){
        if (connected){
            requestStreamObserver.onNext(connectorCommand);
        }
    }

}
