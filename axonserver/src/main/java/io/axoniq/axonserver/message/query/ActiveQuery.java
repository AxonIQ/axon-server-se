/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.message.Cancellable;
import io.axoniq.axonserver.message.FlowControlQueues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    private final Logger logger = LoggerFactory.getLogger(ActiveQuery.class);
    private final String key;
    private final QueryDefinition query;
    private final Consumer<QueryResponse> responseConsumer;
    private final long timestamp = System.currentTimeMillis();
    private final Consumer<String> onCompleted;
    private final Set<QueryHandler<?>> handlers;
    private final Map<QueryHandler<?>, Cancellable> cancelOperations = new ConcurrentHashMap<>();
    private final String sourceClientId;
    private final AtomicReference<QueryResponse> failedResponse = new AtomicReference<>();
    private final boolean streaming;
    private final AtomicReference<String> targetClientStreamId = new AtomicReference<>();
    private final SerializedQuery serializedQuery;
    private final long timeout;

    /**
     * Creates an instance with the specified parameters.
     *
     * @param key              the unique identifier of the query request
     * @param serializedQuery  the serialized query request
     * @param responseConsumer a {@link Consumer} for the received {@link QueryResponse}
     * @param onCompleted      a {@link Consumer} for the clientStreamId that sent the last expected response
     * @param handlers         all handlers applicable to the given query
     */
    public ActiveQuery(String key,
                       SerializedQuery serializedQuery,
                       Consumer<QueryResponse> responseConsumer,
                       Consumer<String> onCompleted,
                       Set<QueryHandler<?>> handlers) {
        this(key, serializedQuery, responseConsumer, onCompleted, handlers, false);
    }

    /**
     * Creates an instance with the specified parameters.
     *
     * @param key              the unique identifier of the query request
     * @param serializedQuery  the serialized query request
     * @param responseConsumer a {@link Consumer} for the received {@link QueryResponse}
     * @param onCompleted    a {@link Consumer} for the clientStreamId that sent the last expected response
     * @param handlers         all handlers applicable to the given query
     * @param streaming        indicates whether results of this query are going to be streamed
     */
    public ActiveQuery(String key,
                       SerializedQuery serializedQuery,
                       Consumer<QueryResponse> responseConsumer,
                       Consumer<String> onCompleted,
                       Set<QueryHandler<?>> handlers,
                       boolean streaming) {
        this.key = key;
        this.sourceClientId = serializedQuery.query().getClientId();
        this.query = new QueryDefinition(serializedQuery.context(), serializedQuery.query().getQuery());
        this.responseConsumer = responseConsumer;
        this.onCompleted = onCompleted;
        this.handlers = new CopyOnWriteArraySet<>(handlers);
        this.streaming = streaming;
        this.serializedQuery = serializedQuery;
        this.timeout = System.currentTimeMillis() + ProcessingInstructionHelper
                .timeout(serializedQuery.query()
                                        .getProcessingInstructionsList());
    }


    /**
     * Returns the query definition
     *
     * @return the query definition
     */
    public QueryDefinition getQuery() {
        return query;
    }

    /**
     * Returns the query timestamp
     *
     * @return the query timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Dispatches the query request to all handlers.
     *
     * @param queues the destinations' queues used to buffer the query instructions to be sent
     */
    public void dispatchQuery(FlowControlQueues<QueryInstruction> queues) {
        handlers.forEach(handler -> cancelOperations
                .computeIfAbsent(handler, h -> h.enqueueQuery(serializedQuery, queues, timeout, streaming)));
    }

    /**
     * Forwards the {@link QueryResponse} to the response consumer. The response was sent by the client with given
     * {@code clientStreamId}.
     *
     * @param queryResponse  the {@link QueryResponse}
     * @param clientStreamId the identifier of the target client stream
     * @return {@code true} if forwarding was successful, {@code false} otherwise
     */
    public boolean forward(QueryResponse queryResponse, String clientStreamId) {
        if (!singleTargetForStreamingQueries(clientStreamId)) {
            return false;
        }
        forward(queryResponse);
        return true;
    }

    /**
     * Forwards the {@link QueryResponse} to the response consumer.
     *
     * @param queryResponse the {@link QueryResponse}
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

    private boolean singleTargetForStreamingQueries(String clientStreamId) {
        if (streaming) {
            targetClientStreamId.compareAndSet(null, clientStreamId);
            return clientStreamId.equals(targetClientStreamId.get());
        }
        return true;
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
    public boolean complete(String clientStreamId) {
        for (QueryHandler<?> handler : handlers) {
            if (clientStreamId.equals(handler.getClientStreamId())) {
                if (cancelOperations.containsKey(handler)) {
                    cancelOperations.remove(handler).cancel();
                }
                handlers.remove(handler);
            }
        }
        if (handlers.isEmpty()) {
            QueryResponse response = failedResponse.getAndSet(null);
            if (response != null) {
                responseConsumer.accept(response);
            }
            String message = "Completed last target client stream id: " + clientStreamId;
            logger.debug("There are no handlers anymore for the query {}, so it will be completed with this message: {}",
                         serializedQuery.getMessageIdentifier(),
                         message);
            onCompleted.accept(message);
        }
        return handlers.isEmpty();
    }

    /**
     * Request the unique identifier for the query request
     *
     * @return the unique identifier for the query request
     */
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
        logger.debug("Cancelling query {} with errorCode: {}, errorMessage: {}",
                     serializedQuery.getMessageIdentifier(),
                     errorCode,
                     message);
        responseConsumer.accept(buildErrorResponse(errorCode, message));
        cancel();
    }

    /**
     * Cancels the request to all handlers and completes the query request.
     */
    public void cancel() {
        logger.debug("Cancelling query {}.",
                     serializedQuery.getMessageIdentifier());
        try {
            handlers.forEach(handler -> {
                Cancellable cancellable = cancelOperations.remove(handler);
                if (cancellable != null) {
                    logger.debug("Forwarding cancel for the query {} to the query handler with clientStreamId = {}",
                                 serializedQuery.getMessageIdentifier(),
                                 handler.getClientStreamId());
                    cancellable.cancel();
                }
            });
            handlers.clear();
            logger.debug("Completing query {} as cancelled.",
                         serializedQuery.getMessageIdentifier());
            onCompleted.accept("Cancelled");
        } catch (RuntimeException ignore) {
            // ignore exception on cancel
        }
    }

    /**
     * Returns the context for the query
     *
     * @return the context for the query
     */
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

    /**
     * Completes the request for the handler identified by the {@code clientStreamId} with the specified error.
     *
     * @param clientStreamId the identifier of the query handler
     * @param errorCode      the error code
     * @param message        the error message.
     * @return {@code true} if the query request has been completed, {@code false} if there are other handlers still active.
     */
    public boolean completeWithError(String clientStreamId, ErrorCode errorCode, String message) {
        responseConsumer.accept(buildErrorResponse(errorCode, message));
        return complete(clientStreamId);
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
     * Cancels handlers that do not match given {@code clientStreamId}.
     *
     * @param clientStreamId the identifier of the client stream
     */
    public void cancelOtherHandlersBut(String clientStreamId) {
        logger.debug("Cancelling all query handlers for query {} but the one for clientStreamId {}",
                     serializedQuery.getMessageIdentifier(),
                     clientStreamId);
        Set<QueryHandler<?>> toRemove = handlers.stream()
                                                .filter(h -> !clientStreamId.equals(h.getClientStreamId()))
                                                .collect(Collectors.toSet());
        toRemove.forEach(handler -> {
            Cancellable cancellable = cancelOperations.remove(handler);
            if (cancellable != null) {
                cancellable.cancel();
            }
        });
        handlers.removeAll(toRemove);
    }

    /**
     * @return the name of the query.
     */
    public String queryName() {
        return query.getQueryName();
    }
}
