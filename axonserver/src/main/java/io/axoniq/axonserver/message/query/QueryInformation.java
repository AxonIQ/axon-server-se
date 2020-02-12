/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.query.QueryResponse;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public class QueryInformation {

    private final String key;
    private final QueryDefinition query;
    private final Consumer<QueryResponse> responseConsumer;
    private final long timestamp = System.currentTimeMillis();
    private final AtomicInteger remainingReplies;
    private final Consumer<String> onAllReceived;
    private final Set<String> handlerNames;
    private final String sourceClientId;

    public QueryInformation(String key, String sourceClientId, QueryDefinition query, Set<String> handlerNames,
                            int expectedResults,
                            Consumer<QueryResponse> responseConsumer, Consumer<String> onAllReceived) {
        this.key = key;
        this.sourceClientId = sourceClientId;
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
            if( queryResponse.hasErrorMessage()) completed(client);
            if (remaining <= 0) {
                onAllReceived.accept(client);
            }
        } catch( RuntimeException ignored) {
            // ignore exception on sending response, caused by other party already gone
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
        } catch (RuntimeException ignore) {
            // ignore exception on cancel
        }
    }

    public String getContext() {
        return query.getContext();
    }

    public boolean waitingFor(String client) {
        return handlerNames.contains(client);
    }

    public boolean completeWithError(String client, ErrorCode errorCode, String message) {
        responseConsumer.accept(QueryResponse.newBuilder()
                                             .setErrorCode(errorCode.getCode())
                                             .setErrorMessage(ErrorMessage.newBuilder().setMessage(message))
                                             .setRequestIdentifier(key)
                                             .build());

        return completed(client);
    }

    public String getSourceClientId() {
        return sourceClientId;
    }

    public Set<String> waitingFor() {
        return handlerNames;
    }
}
