/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;


import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.Cancellable;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Basic handler for queries. Puts a query in a specific queue to send it based on its priority to the target client.
 * @author Marc Gathier
 * @since 4.0
 */
public abstract class QueryHandler<T> {

    private final Logger logger = LoggerFactory.getLogger(QueryHandler.class);
    private final ClientStreamIdentification clientStreamIdentification;
    private final String componentName;
    private final String clientId;
    protected final StreamObserver<T> streamObserver;

    protected QueryHandler(StreamObserver<T> streamObserver,
                           ClientStreamIdentification clientStreamIdentification,
                           String componentName, String clientId) {
        this.clientStreamIdentification = clientStreamIdentification;
        this.streamObserver = streamObserver;
        this.componentName = componentName;
        this.clientId = clientId;
    }

    /**
     * Directly sends a query (initial query for a subscription query to the target client)
     *
     * @param query the query to send
     */
    public abstract void dispatch(SubscriptionQueryRequest query);

    /**
     * Returns the identification of the client that subscribed the handler
     *
     * @return the identification of the client that subscribed the handler
     */
    public ClientStreamIdentification getClientStreamIdentification() {
        return clientStreamIdentification;
    }

    /**
     * Returns the name of the component that subscribed the handler
     *
     * @return the name of the component that subscribed the handler
     */
    public String getComponentName() {
        return componentName;
    }

    /**
     * Returns the name of the query that this handler can receive
     *
     * @return the name of the query that this handler can receive
     */
    public String queueName() {
        return clientStreamIdentification.toString();
    }

    public String toString() {
        return clientStreamIdentification.toString();
    }

    /**
     * Enqueues the query for later dispatching.
     *
     * @param request    the serialized query request
     * @param queryQueue the queue used for enqueueing
     * @param timeout    how long we should wait for this query
     * @param streaming  indicates whether this query is streaming results or not
     * @return the function to cancel the query, removing the request from the queue if still possible or enqueueing a
     * cancel instruction otherwise
     */
    public Cancellable enqueueQuery(SerializedQuery request, FlowControlQueues<QueryInstruction> queryQueue,
                                    long timeout,
                                    boolean streaming) {
        logger.trace("Enqueueing query request instruction {} for target client {}.",
                     request.getMessageIdentifier(), clientStreamIdentification);
        QueryInstruction.Query query = new QueryInstruction.Query(getClientStreamIdentification(),
                                                                  getClientId(),
                                                                  request.withClient(getClientStreamId()),
                                                                  timeout,
                                                                  0L,
                                                                  streaming);
        Cancellable cancellable = enqueueInstruction(queryQueue, QueryInstruction.query(query));
        return () -> {
            logger.debug("Cancelling the query request {} for target client {}.",
                         request.getMessageIdentifier(),
                         clientStreamIdentification);
            if (!cancellable.cancel()) {
                logger.trace("Enqueueing cancel instruction to target client {} for query {}.",
                             clientStreamIdentification, request.getMessageIdentifier());
                enqueueCancellation(request.getMessageIdentifier(), request.query().getQuery(), queryQueue);
            } else {
                logger.trace("Query request {} removed from sending queue to target client {}.",
                             request.getMessageIdentifier(),
                             clientStreamIdentification);
            }
            return true;
        };
    }

    /**
     * Enqueues cancellation for the query with given {@code requestId} and {@code queryName}.
     *
     * @param requestId  the identifier of the query request
     * @param queryName  the name of the query
     * @param queryQueue the queue used for enqueueing
     */
    private void enqueueCancellation(String requestId, String queryName,
                                     FlowControlQueues<QueryInstruction> queryQueue) {
        QueryInstruction.Cancel cancel = new QueryInstruction.Cancel(requestId,
                                                                     queryName,
                                                                     getClientStreamIdentification());
        enqueueInstruction(queryQueue, QueryInstruction.cancel(cancel));
    }

    /**
     * Enqueues flow control of {@code permits} for the query with given {@code requestId} and {@code queryName}.
     *
     * @param requestId  the identifier of the query request
     * @param queryName  the name of the query
     * @param permits    the permits - how many results we are demanindg from this handler to produce
     * @param queryQueue the queue used for enqueueing
     */
    public void enqueueFlowControl(String requestId, String queryName, long permits,
                                   FlowControlQueues<QueryInstruction> queryQueue) {
        QueryInstruction.FlowControl flowControl = new QueryInstruction.FlowControl(requestId,
                                                                                    queryName,
                                                                                    getClientStreamIdentification(),
                                                                                    permits);
        enqueueInstruction(queryQueue, QueryInstruction.flowControl(flowControl));
    }

    /**
     * Enqueues the given {@code instruction} to the given {@code queryQueue}.
     *
     * @param queryQueue  the queue used for enqueueing
     * @param instruction the query instruction
     *
     * @return the function to remove the instruction from the queue
     */
    public Cancellable enqueueInstruction(FlowControlQueues<QueryInstruction> queryQueue,
                                          QueryInstruction instruction) {
        return queryQueue.put(queueName(), instruction, instruction.priority());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryHandler<?> that = (QueryHandler<?>) o;
        return Objects.equals(clientStreamIdentification, that.clientStreamIdentification);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientStreamIdentification);
    }

    /**
     * Returns the id of the query stream to the client that subscribed the handler
     *
     * @return the id of the query stream to the client that subscribed the handler
     */
    public String getClientStreamId() {
        return clientStreamIdentification.getClientStreamId();
    }

    /**
     * Returns the unique name of the client the subscribed the handler
     *
     * @return the unique name of the client the subscribed the handler
     */
    public String getClientId() {
        return clientId;
    }
}
