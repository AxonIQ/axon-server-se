package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.ProcessingKey;
import io.axoniq.axonserver.grpc.ReceivingStreamObserver;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.grpc.internal.ClientStatus;
import io.axoniq.axonserver.grpc.internal.ClientSubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.internal.ConnectRequest;
import io.axoniq.axonserver.grpc.internal.ConnectResponse;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.ForwardedCommand;
import io.axoniq.axonserver.grpc.internal.ForwardedCommandResponse;
import io.axoniq.axonserver.grpc.internal.ForwardedQuery;
import io.axoniq.axonserver.grpc.internal.InternalCommandSubscription;
import io.axoniq.axonserver.grpc.internal.InternalQuerySubscription;
import io.axoniq.axonserver.grpc.internal.QueryComplete;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.subscription.UpdateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds connection to other axonserver platform node. Receives commands and queries from the other node to execute.
 * Each subscription made to this node is forwarded to the connected node.
 * <p>
 * Managed by {@link ClusterController}, which will check connection status and try to reconnect lost connections.
 * @author Marc Gathier
 */
public class RemoteConnection  {

    private static final Logger logger = LoggerFactory.getLogger(RemoteConnection.class);
    private static final int IGNORE_SAME_ERROR_COUNT = 10;
    private final ClusterController clusterController;
    private final ClusterNode clusterNode;
    private final StubFactory stubFactory;
    private final QueryDispatcher queryDispatcher;
    private final CommandDispatcher commandDispatcher;
    private AtomicBoolean connected = new AtomicBoolean();
    private AtomicLong connectionPending = new AtomicLong();
    private volatile ClusterFlowControlStreamObserver requestStreamObserver;
    private volatile String errorMessage;
    private final long connectionWaitTime;
    private final AtomicInteger repeatedErrorCount = new AtomicInteger(IGNORE_SAME_ERROR_COUNT);

    public RemoteConnection(ClusterController clusterController, ClusterNode clusterNode,
                            StubFactory stubFactory,
                            QueryDispatcher queryDispatcher,
                            CommandDispatcher commandDispatcher) {
        this.clusterController = clusterController;
        this.clusterNode = clusterNode;
        this.stubFactory = stubFactory;
        this.queryDispatcher = queryDispatcher;
        this.commandDispatcher = commandDispatcher;
        this.connectionWaitTime = clusterController.getConnectionWaitTime();
    }

    public synchronized RemoteConnection init() {
        logger.debug("Connecting to: {}:{}", clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort());
        if (!validateConnection()) {
            return this;
        }
        requestStreamObserver = new ClusterFlowControlStreamObserver(stubFactory.messagingClusterServiceStub( clusterNode)
                .openStream(new ReceivingStreamObserver<ConnectorResponse>(logger) {
                    @Override
                    protected void consume(ConnectorResponse connectorResponse) {
                        checkConnected();

                        switch (connectorResponse.getResponseCase()) {
                                case CONFIRMATION:
                                    break;

                                case COMMAND:
                                    ForwardedCommand forwardedCommand = connectorResponse.getCommand();
                                    commandDispatcher.dispatch(forwardedCommand.getContext(),
                                                               new SerializedCommand(forwardedCommand.getCommand().toByteArray(),
                                                                                     forwardedCommand.getClient(),
                                                                                     forwardedCommand.getMessageId()),
                                                               commandResponse -> publish(
                                                                       ConnectorCommand.newBuilder()
                                                                                       .setCommandResponse(
                                                                                               ForwardedCommandResponse
                                                                                                       .newBuilder().setRequestIdentifier(commandResponse.getRequestIdentifier())
                                                                                                       .setResponse(commandResponse.toByteString()).build())
                                                                                       .build()),
                                                               true);

                                    break;
                                case QUERY:
                                    query(connectorResponse.getQuery());
                                    break;

                                case CONNECT_RESPONSE:
                                    connectResponse(connectorResponse.getConnectResponse());
                                    break;

                                case SUBSCRIPTION_QUERY_REQUEST:
                                    ClientSubscriptionQueryRequest request = connectorResponse.getSubscriptionQueryRequest();
                                    UpdateHandler handler = new ProxyUpdateHandler(requestStreamObserver::onNext);
                                    clusterController.publishEvent(
                                            new SubscriptionQueryEvents.ProxiedSubscriptionQueryRequest(request.getSubscriptionQueryRequest(),
                                                                                                        handler,
                                                                                                        request.getClient()));
                                    break;
                                default:
                                    break;
                            }
                    }

                    private void checkConnected() {
                        if (connected.compareAndSet(false, true)) {
                            logger.debug("Connected to {}:{}", clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort());
                            errorMessage = null;
                            connectionPending.set(0);
                            initFlowControl();
                        }
                    }

                    private void query(ForwardedQuery query) {
                        SerializedQuery serializedQuery = new SerializedQuery(query.getContext(),
                                                                              query.getClient(),
                                                                              query.getQuery().toByteArray());
                        queryDispatcher.dispatchProxied(serializedQuery,
                                                                 queryResponse -> sendQueryResponse(
                                                                         serializedQuery.client(),
                                                                         queryResponse),
                                                                 client -> sendQueryComplete(
                                                                         serializedQuery.getMessageIdentifier(),
                                                                         client));
                    }

                    private void connectResponse(ConnectResponse connectResponse) {
                        logger.debug("Connected, received response: {}",
                                     connectResponse);

                        try {
                            if( connectResponse.getDeleted()) {
                                clusterController.requestDelete(clusterNode.getName());
                            } else {
                                clusterController
                                        .publishEvent(new ClusterEvents.AxonServerInstanceConnected(
                                                RemoteConnection.this));
                            }

                        } catch (Exception ex) {
                            logger.warn("Failed to process request {}",
                                        connectResponse,
                                        ex);
                        }
                    }

                    @Override
                    protected String sender() {
                        return clusterNode.getName();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        if (!String.valueOf(throwable.getMessage()).equals(errorMessage) || repeatedErrorCount.decrementAndGet() <= 0) {
                            ManagedChannelHelper.checkShutdownNeeded(clusterNode.getName(), throwable);
                            logger.warn("Error on {}:{} - {}", clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort(), throwable.getMessage());
                            errorMessage = String.valueOf(throwable.getMessage());
                            repeatedErrorCount.set(IGNORE_SAME_ERROR_COUNT);
                        }
                        closeConnection();
                        logger.debug("Connected: {}, connection pending: {}", connected, connectionPending);
                    }

                    private void closeConnection() {
                        if( connected.compareAndSet(true, false)) {
                            clusterController.publishEvent(new ClusterEvents.AxonServerInstanceDisconnected(clusterNode.getName()));
                            connectionPending.set(0);
                        }
                    }

                    @Override
                    public void onCompleted() {
                        logger.debug("Completed connection to {}:{}", clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort());
                        closeConnection();
                    }
                }));
        requestStreamObserver.onNext(ConnectorCommand.newBuilder()
                .setConnect( ConnectRequest.newBuilder()
                                           .setNodeInfo(clusterController.getMe().toNodeInfo())
                                           .setAdmin(clusterController.getMe().isAdmin())
                                           .build())

                .build());

        // send master info
        connectionPending.set(System.currentTimeMillis());
        return this;
    }

    private boolean validateConnection() {
        try {
            InetAddress[] addresses = InetAddress.getAllByName(clusterNode.getInternalHostName());
            logger.debug("Connect to {}", addresses[0]);
        } catch (UnknownHostException e) {
            if (!String.valueOf(e.getMessage()).equals(errorMessage) || repeatedErrorCount.decrementAndGet() <= 0) {
                logger.warn("Unknown host: {}", clusterNode.getInternalHostName());
                errorMessage = String.valueOf(e.getMessage());
                repeatedErrorCount.set(IGNORE_SAME_ERROR_COUNT);
            }
            return false;
        }
        return true;
    }

    private void initFlowControl() {
        requestStreamObserver.initCommandFlowControl(clusterController.getName(), clusterController.getCommandFlowControl());
        requestStreamObserver.initQueryFlowControl(clusterController.getName(), clusterController.getQueryFlowControl());

    }

    private void sendQueryResponse( String client, QueryResponse queryResponse) {
        requestStreamObserver.onNext(ConnectorCommand.newBuilder().setQueryResponse(
                QueryResponse.newBuilder(queryResponse)
                        .addProcessingInstructions(ProcessingInstruction.newBuilder()
                                .setKey(ProcessingKey.TARGET_CLIENT)
                                .setValue(MetaDataValue.newBuilder().setTextValue(client))
                        )
        ).build());
    }

    private void sendQueryComplete( String client, String requestIdentifier) {
        requestStreamObserver.onNext(ConnectorCommand.newBuilder()
                                                     .setQueryComplete(
                                                             QueryComplete.newBuilder()
                                                                          .setMessageId(requestIdentifier)
                                                                          .setClient(client))
                                                     .build());
    }

    public void close() {
        if (connected.compareAndSet(true, false) && requestStreamObserver != null) {
            requestStreamObserver.onCompleted();
        }
    }

    public boolean isConnected() {
        return connected.get();
    }

    public void checkConnection() {
        if( logger.isDebugEnabled() && System.currentTimeMillis() - connectionPending.get() < connectionWaitTime)
            logger.debug("Connection pending to: {}:{}", clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort());
        if (!connected.get() && System.currentTimeMillis() - connectionPending.get() > connectionWaitTime) {
            init();
        }
    }

    public void unsubscribeCommand(String context, String command, String client, String componentName) {
            publish(ConnectorCommand.newBuilder()
                    .setUnsubscribeCommand(InternalCommandSubscription.newBuilder()
                            .setCommand(
                                    CommandSubscription.newBuilder()
                                            .setClientId(client)
                                            .setCommand(command)
                                            .setComponentName(componentName)
                            ).setContext(context))
                    .build());

    }

    public void subscribeCommand(String context, String command, String client, String componentName) {
            publish(ConnectorCommand.newBuilder()
                    .setSubscribeCommand(InternalCommandSubscription.newBuilder()
                            .setCommand(CommandSubscription.newBuilder()
                                    .setClientId(client)
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
                                                         .setClientId(clientName)
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
                                    .setClientId(client)
                                    .setQuery(queryDefinition.getQueryName())
                                    .setComponentName(componentName))
                            .setContext(queryDefinition.getContext())
                    ).build());
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
        if (connected.get()){
            requestStreamObserver.onNext(connectorCommand);
        }
    }

}
