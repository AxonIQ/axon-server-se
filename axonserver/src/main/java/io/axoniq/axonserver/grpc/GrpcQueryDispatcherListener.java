package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.WrappedQuery;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads messages for a specific client from a queue and sends them to the client using gRPC.
 * Only reads messages when there are permits left.
 * @author Marc Gathier
 */
public class GrpcQueryDispatcherListener extends GrpcFlowControlledDispatcherListener<QueryProviderInbound,WrappedQuery> implements QueryRequestValidator {
    private static final Logger logger = LoggerFactory.getLogger(GrpcQueryDispatcherListener.class);
    private final QueryDispatcher queryDispatcher;

    public GrpcQueryDispatcherListener(QueryDispatcher queryDispatcher, String client, StreamObserver<QueryProviderInbound> queryProviderInboundStreamObserver, int threads) {
        super(queryDispatcher.getQueryQueue(), client, queryProviderInboundStreamObserver, threads);
        this.queryDispatcher = queryDispatcher;
    }

    @Override
    protected boolean send(WrappedQuery message) {
        if( logger.isDebugEnabled()) {
            logger.debug("Send request {}, with priority: {}", message.queryRequest(), message.priority() );
        }
        SerializedQuery request = validate(message, queryDispatcher, logger);
        if( request == null) return false;
        inboundStream.onNext(QueryProviderInbound.newBuilder().setQuery(request.query()).build());
        return true;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}

