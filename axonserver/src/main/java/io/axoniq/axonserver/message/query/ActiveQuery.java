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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Contains all necesseary information about active query - a query in progress.
 *
 * @author Milan Savic
 * @author Marc Gathier
 */
public class ActiveQuery {

    private final String key;
    private final QueryDefinition query;
    private final Consumer<QueryResponse> responseConsumer;
    private final long timestamp = System.currentTimeMillis();
    private final Consumer<String> onAllReceived;
    private final Set<QueryHandler<?>> handlers;
    private final String sourceClientId;
    private final AtomicReference<QueryResponse> failedResponse = new AtomicReference<>();
    private final boolean streaming;

    /**
     * Creates an instance with the specified parameters.
     *
     * @param key              the unique identifier of the query request
     * @param sourceClientId   the unique identifier of the client that sent the query
     * @param query            the {@link QueryDefinition}
     * @param responseConsumer a {@link Consumer} for the received {@link QueryResponse}
     * @param onAllReceived    a {@link Consumer} for the clientStreamId that sent the last expected response
     * @param handlers         all handlers applicable to the given query
     */
    public ActiveQuery(String key,
                       String sourceClientId,
                       QueryDefinition query,
                       Consumer<QueryResponse> responseConsumer,
                       Consumer<String> onAllReceived,
                       Set<QueryHandler<?>> handlers) {
        this(key, sourceClientId, query, responseConsumer, onAllReceived, handlers, false);
    }

    /**
     * Creates an instance with the specified parameters.
     *
     * @param key              the unique identifier of the query request
     * @param sourceClientId   the unique identifier of the client that sent the query
     * @param query            the {@link QueryDefinition}
     * @param responseConsumer a {@link Consumer} for the received {@link QueryResponse}
     * @param onAllReceived    a {@link Consumer} for the clientStreamId that sent the last expected response
     * @param handlers         all handlers applicable to the given query
     * @param streaming        indicates whether results of this query are going to be streamed
     */
    public ActiveQuery(String key,
                       String sourceClientId,
                       QueryDefinition query,
                       Consumer<QueryResponse> responseConsumer,
                       Consumer<String> onAllReceived,
                       Set<QueryHandler<?>> handlers,
                       boolean streaming) {
        this.key = key;
        this.sourceClientId = sourceClientId;
        this.query = query;
        this.responseConsumer = responseConsumer;
        this.onAllReceived = onAllReceived;
        this.handlers = new CopyOnWriteArraySet<>(handlers);
        this.streaming = streaming;
    }

    public QueryDefinition getQuery() {
        return query;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Forwards the {@link QueryResponse} to the response consumer.
     *
     * @param queryResponse  the {@link QueryResponse}
     */
    public void forward(QueryResponse queryResponse) {
        try {
            if (hasError(queryResponse)) {
                failedResponse.set(queryResponse);
            } else {
                responseConsumer.accept(queryResponse);
                failedResponse.set(null);
            }
        } catch (RuntimeException ignored) {
            // ignore exception on sending response, caused by other party already gone
        }
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
        handlers.removeIf(h -> clientStreamId.equals(h.getClientStreamId()));
        if (handlers.isEmpty()) {
            QueryResponse response = failedResponse.getAndSet(null);
            if (response != null) {
                responseConsumer.accept(response);
            }
            onAllReceived.accept(clientStreamId);
        }
        return handlers.isEmpty();
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
            handlers.clear();
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
        return handlers.stream()
                       .anyMatch(h -> clientStreamId.equals(h.getClientStreamId()));
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
        return handlers.stream()
                       .map(QueryHandler::getClientStreamId)
                       .collect(Collectors.toSet());
    }

    /**
     * @return {@code true} if results of this query are going to be streamed, {@code false} otherwise
     */
    public boolean isStreaming() {
        return streaming;
    }

    /**
     * @return a set of all applicable handlers for this query. The set is not modifiable.
     */
    public Set<QueryHandler<?>> handlers() {
        return Collections.unmodifiableSet(handlers);
    }

    /**
     * @return the name of the query.
     */
    public String queryName() {
        return query.getQueryName();
    }
}
