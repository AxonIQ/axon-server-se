package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.grpc.stub.StreamObserver;

/**
 * Author: marc
 */
public class DirectQueryHandler extends QueryHandler<QueryProviderInbound> {

    public DirectQueryHandler(StreamObserver<QueryProviderInbound> streamObserver, String clientName, String componentName) {
        super(streamObserver, clientName, componentName);
    }

    @Override
    public void dispatch(SubscriptionQueryRequest query) {
        streamObserver.onNext(QueryProviderInbound.newBuilder()
                                                  .setSubscriptionQueryRequest(query)
                                                  .build());
    }
}
