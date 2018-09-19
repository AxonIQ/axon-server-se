package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.SendingStreamObserver;
import io.axoniq.axonserver.grpc.internal.ClientSubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.WrappedQuery;

/**
 * Handler for a query to be sent to another AxonHub node.
 * Updates the message with a new message id and adds the original message id in the processing instructions, as one query may
 * lead to multiple messages to be sent to other AxonHub nodes.
 * Also adds the target client to the processing instructions, so the target AxonHub node knows where to send the request to.
 * Author: marc
 */
public class ProxyQueryHandler extends QueryHandler<ConnectorResponse> {
    private final String messagingServerName;

    public ProxyQueryHandler(SendingStreamObserver<ConnectorResponse> responseObserver, String clientName, String componentName, String messagingServerName) {
        super(responseObserver, clientName, componentName);
        this.messagingServerName = messagingServerName;
    }

    public String getMessagingServerName() {
        return messagingServerName;
    }

    @Override
    public void dispatch(QueryRequest query) {
        streamObserver.onNext(ConnectorResponse.newBuilder()
                .setQuery(QueryRequest.newBuilder(query)
                        .addProcessingInstructions(ProcessingInstructionHelper.targetClient(getClientName()))
                ).build());
    }

    @Override
    public void dispatch(SubscriptionQueryRequest query) {
        ClientSubscriptionQueryRequest request = ClientSubscriptionQueryRequest.newBuilder()
                                                                             .setClient(this.getClientName())
                                                                             .setSubscriptionQueryRequest(query)
                                                                             .build();
        streamObserver.onNext(ConnectorResponse.newBuilder().setSubscriptionQueryRequest(request).build());
    }

    @Override
    public String toString() {
        return getClientName() + "@" + messagingServerName;
    }

    @Override
    public void enqueue(String context, QueryRequest request, FlowControlQueues<WrappedQuery> commandQueue, long timeout) {
        QueryRequest updated = QueryRequest.newBuilder(request)
                .addProcessingInstructions(ProcessingInstructionHelper.targetClient(getClientName()))
                .build();
        commandQueue.put(messagingServerName, new WrappedQuery(context, updated, timeout));
    }

}
