package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientIdentification;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class DirectQueryHandler extends QueryHandler<QueryProviderInbound> {

    public DirectQueryHandler(StreamObserver<QueryProviderInbound> streamObserver, ClientIdentification clientIdentification, String componentName) {
        super(streamObserver, clientIdentification, componentName);
    }

    @Override
    public void dispatch(SubscriptionQueryRequest query) {
        streamObserver.onNext(QueryProviderInbound.newBuilder()
                                                  .setSubscriptionQueryRequest(query)
                                                  .build());
    }
}
