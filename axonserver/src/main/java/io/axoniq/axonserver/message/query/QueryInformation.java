package io.axoniq.axonserver.message.query;

import io.axoniq.axonhub.QueryResponse;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public class QueryInformation {

    private final String key;
    private final QueryDefinition query;
    private final Consumer<QueryResponse> responseConsumer;
    private final long timestamp = System.currentTimeMillis();
    private final AtomicInteger remainingReplies;
    private final Consumer<String> onAllReceived;
    private final Set<String> handlerNames;

    public QueryInformation(String key, QueryDefinition query, Set<String> handlerNames, int expectedResults,
                            Consumer<QueryResponse> responseConsumer, Consumer<String> onAllReceived) {
        this.key = key;
        this.query = query;
        this.responseConsumer = responseConsumer;
        this.remainingReplies = new AtomicInteger(expectedResults);
        this.onAllReceived = onAllReceived;
        this.handlerNames = new CopyOnWriteArraySet<>(handlerNames);
    }

    public QueryDefinition getQuery() {
        return query;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int forward(String client, QueryResponse queryResponse) {
        int remaining =0;
        try {
            responseConsumer.accept(queryResponse);
            remaining = remainingReplies.decrementAndGet();
            if (remaining <= 0) {
                onAllReceived.accept(client);
            }
        } catch( Throwable ignored) {

        }
        return remaining;
    }

    public boolean completed(String client) {
        handlerNames.remove(client);
        if( handlerNames.isEmpty()) onAllReceived.accept(client);
        return handlerNames.isEmpty();
    }

    public String getKey() {
        return key;
    }

    public void cancel() {
        try {
            handlerNames.clear();
            onAllReceived.accept("Cancelled");
        } catch (Throwable ignore) {}
    }
}
