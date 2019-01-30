package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import io.axoniq.axonserver.grpc.QueryRequestValidator;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.ForwardedQuery;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.WrappedQuery;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads messages for a specific messagingServerName from a queue and sends them to the messagingServerName using gRPC.
 * Only reads messages when there are permits left.
 * @author Marc Gathier
 */
public class GrpcInternalQueryDispatcherListener extends GrpcFlowControlledDispatcherListener<ConnectorResponse, WrappedQuery> implements QueryRequestValidator {
    private static final Logger logger = LoggerFactory.getLogger(GrpcInternalQueryDispatcherListener.class);
    private final QueryDispatcher queryDispatcher;

    public GrpcInternalQueryDispatcherListener(QueryDispatcher queryDispatcher, String messagingServerName, StreamObserver<ConnectorResponse> commandProviderInboundStreamObserver, int threads) {
        super(queryDispatcher.getQueryQueue(), messagingServerName, commandProviderInboundStreamObserver, threads);
        this.queryDispatcher = queryDispatcher;
    }

    @Override
    protected boolean send(WrappedQuery message) {
        SerializedQuery request = validate(message, queryDispatcher, logger);
        if( request == null) return false;

        inboundStream.onNext(ConnectorResponse.newBuilder()
                                              .setQuery(
                                                    ForwardedQuery.newBuilder()
                                                                  .setClient(message.client().getClient())
                                                                  .setContext(message.client().getContext())
                                                                  .setQuery(request.toByteString()))
                                              .build());
        return true;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
