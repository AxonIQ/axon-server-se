package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.grpc.query.QueryResponse;
/**
 * @author Marc Gathier
 */
public interface QueryResponseConsumer {
    void onNext(QueryResponse queryResponse);

    void onCompleted();
}
