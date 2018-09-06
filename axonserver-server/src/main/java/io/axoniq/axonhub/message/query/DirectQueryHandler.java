package io.axoniq.axonhub.message.query;

import io.axoniq.axonhub.QueryRequest;
import io.axoniq.axonhub.SubscriptionQueryRequest;
import io.axoniq.axonhub.grpc.QueryProviderInbound;
import io.grpc.stub.StreamObserver;

/**
 * Author: marc
 */
public class DirectQueryHandler extends QueryHandler<QueryProviderInbound> {

    public DirectQueryHandler(StreamObserver<QueryProviderInbound> streamObserver, String clientName, String componentName) {
        super(streamObserver, clientName, componentName);
    }

    @Override
    public void dispatch(QueryRequest query) {
            streamObserver.onNext(QueryProviderInbound.newBuilder()
                    .setQuery(query)
                    .build());
    }

    @Override
    public void dispatch(SubscriptionQueryRequest query) {
        streamObserver.onNext(QueryProviderInbound.newBuilder()
                                                  .setSubscriptionQueryRequest(query)
                                                  .build());
    }
}
