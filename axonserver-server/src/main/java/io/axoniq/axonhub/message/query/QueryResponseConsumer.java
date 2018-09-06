package io.axoniq.axonhub.message.query;

import io.axoniq.axonhub.QueryResponse;
/**
 * Author: marc
 */
public interface QueryResponseConsumer {
    void onNext(QueryResponse queryResponse);

    void onCompleted();
}
