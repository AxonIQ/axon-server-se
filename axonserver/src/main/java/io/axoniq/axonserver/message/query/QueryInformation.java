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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

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
    private final Set<String> clientStreamIds;
    private final String sourceClientId;
    private final AtomicReference<QueryResponse> failedResponse = new AtomicReference<>();

    /**
     * Creates an instance with the specified parameters.
     *
     * @param key              the unique identifier of the query request
     * @param sourceClientId   the unique identifier of the client that sent the query
     * @param query            the {@link QueryDefinition}
     * @param clientStreamIds  the unique identifiers of the query stream opened by the clients that subscribed an
     *                         handler for the query
     * @param expectedResults  the number of the result that are expected
     * @param responseConsumer a {@link Consumer} for the received {@link QueryResponse}
     * @param onAllReceived    a {@link Consumer} for the clientStreamId that sent the last expected response
     */
    public QueryInformation(String key,
                            String sourceClientId,
                            QueryDefinition query,
                            Set<String> clientStreamIds,
                            int expectedResults,
                            Consumer<QueryResponse> responseConsumer,
                            Consumer<String> onAllReceived) {
        this.key = key;
        this.sourceClientId = sourceClientId;
        this.query = query;
        this.responseConsumer = responseConsumer;
        this.remainingReplies = new AtomicInteger(expectedResults);
        this.onAllReceived = onAllReceived;
        this.clientStreamIds = new CopyOnWriteArraySet<>(clientStreamIds);
    }

    public QueryDefinition getQuery() {
        return query;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Handles the {@link QueryResponse} received by the specified {@code clientStreamId}
     *
     * @param clientStreamId the unique identifier of the query stream that the client used to send the response
     * @param queryResponse  the {@link QueryResponse}
     * @return the number of remaining expected response
     */
    public int forward(String clientStreamId, QueryResponse queryResponse) {
        int remaining = remainingReplies.get();
        try {
            if(hasError(queryResponse)) {
                failedResponse.set(queryResponse);
            } else {
                remaining = remainingReplies.decrementAndGet();
                if( remaining >= 0) {
                    responseConsumer.accept(queryResponse);
                    failedResponse.set(null);
                }
            }

            if (remaining <= 0) {
                onAllReceived.accept(clientStreamId);
            }
        } catch (RuntimeException ignored) {
            // ignore exception on sending response, caused by other party already gone
        }
        return remaining;
    }

    private boolean hasError(QueryResponse queryResponse) {
        return !queryResponse.getErrorCode().isEmpty() || queryResponse.hasErrorMessage();
    }

    /**
     * Removes the specified {@code clientStreamId} from the list of the expected ones. If this is the last expected,
     * it invokes the {@code onAllReceived} consumer with the specified {@code clientStreamId}. Returns {@code true} if
     * this was the last expected response, {@code false} if at least another response is expected.
     *
     * @param clientStreamId the unique identifier of the query client stream from which it has been receive a response
     * @return {@code true} if this was the last expected response, {@code false} if at least another response is expected
     */
    public boolean completed(String clientStreamId) {
        clientStreamIds.remove(clientStreamId);
        if (clientStreamIds.isEmpty()) {
            QueryResponse response = failedResponse.getAndSet(null);
            if (response != null) {
                responseConsumer.accept(response);
            }
            onAllReceived.accept(clientStreamId);
        }
        return clientStreamIds.isEmpty();
    }

    public String getKey() {
        return key;
    }

    /**
     * Cancel a query with a specific error, defined by a code and a message.
     *
     * @param errorCode the error code of the cause for canceling
     * @param message   the error message
     */
    public void cancelWithError(ErrorCode errorCode, String message) {
        responseConsumer.accept(buildErrorResponse(errorCode, message));
        cancel();
    }

    public void cancel() {
        try {
            clientStreamIds.clear();
            onAllReceived.accept("Cancelled");
        } catch (RuntimeException ignore) {
            // ignore exception on cancel
        }
    }

    public String getContext() {
        return query.getContext();
    }

    /**
     * Returns {@code true}  if the query is waiting for the handler identified by the specified client stream id.
     *
     * @param clientStreamId the unique identifier of the query long living stream for the client that provides the
     *                       handler.
     * @return {@code true} if the query is waiting for the specified handler, {@code false} otherwise.
     */
    public boolean waitingFor(String clientStreamId) {
        return clientStreamIds.contains(clientStreamId);
    }

    public boolean completeWithError(String clientStreamId, ErrorCode errorCode, String message) {
        responseConsumer.accept(buildErrorResponse(errorCode, message));
        return completed(clientStreamId);
    }

    @Nonnull
    private QueryResponse buildErrorResponse(ErrorCode errorCode, String message) {
        return QueryResponse.newBuilder()
                            .setErrorCode(errorCode.getCode())
                            .setErrorMessage(ErrorMessage.newBuilder().setMessage(message))
                            .setRequestIdentifier(key)
                            .build();
    }

    /**
     * Returns the unique client identifier that sent the query request.
     *
     * @return the unique client identifier that sent the query request.
     */
    public String getSourceClientId() {
        return sourceClientId;
    }

    /**
     * Returns the unique identifiers of the client query stream from which this query is still waiting for a response.
     *
     * @return the unique identifiers of the client query stream from which this query is still waiting for a response.
     */
    public Set<String> waitingFor() {
        return clientStreamIds;
    }
}
