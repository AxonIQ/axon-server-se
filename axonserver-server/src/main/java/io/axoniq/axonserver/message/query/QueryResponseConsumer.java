package io.axoniq.axonserver.message.query;

import io.axoniq.axonhub.QueryResponse;
/**
 * Author: marc
 */
public interface QueryResponseConsumer {
    void onNext(QueryResponse queryResponse);

    void onCompleted();
}
