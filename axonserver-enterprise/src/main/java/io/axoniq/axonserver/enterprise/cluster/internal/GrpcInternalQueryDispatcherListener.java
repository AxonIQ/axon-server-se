package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonhub.QueryRequest;
import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import io.axoniq.axonserver.grpc.QueryRequestValidator;
import io.axoniq.axonhub.internal.grpc.ConnectorResponse;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.WrappedQuery;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads messages for a specific messagingServerName from a queue and sends them to the messagingServerName using gRPC.
 * Only reads messages when there are permits left.
 * Author: marc
 */
public class GrpcInternalQueryDispatcherListener extends GrpcFlowControlledDispatcherListener<ConnectorResponse, WrappedQuery> implements QueryRequestValidator {
    private static final Logger logger = LoggerFactory.getLogger(GrpcInternalQueryDispatcherListener.class);
    private final QueryDispatcher queryDispatcher;

    public GrpcInternalQueryDispatcherListener(QueryDispatcher queryDispatcher, String messagingServerName, StreamObserver<ConnectorResponse> commandProviderInboundStreamObserver) {
        super(queryDispatcher.getQueryQueue(), messagingServerName, commandProviderInboundStreamObserver);
        this.queryDispatcher = queryDispatcher;
    }

    @Override
    protected boolean send(WrappedQuery message) {
        QueryRequest request = validate(message, queryDispatcher, logger);
        if( request == null) return false;

        inboundStream.onNext(ConnectorResponse.newBuilder().setQuery(QueryRequest.newBuilder(request).addProcessingInstructions(ProcessingInstructionHelper.context(message.context()))).build());
        return true;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
