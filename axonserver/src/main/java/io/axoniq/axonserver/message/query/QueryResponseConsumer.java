package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.grpc.query.QueryResponse;
/**
 * Author: marc
 */
public interface QueryResponseConsumer {
    void onNext(QueryResponse queryResponse);

    void onCompleted();
}
