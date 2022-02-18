/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;


import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/**
 * Basic handler for queries. Puts a query in a specific queue to send it based on its priority to the target client.
 * @author Marc Gathier
 * @since 4.0
 */
public abstract class QueryHandler<T> {

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

    public ClientStreamIdentification getClientStreamIdentification() {
        return clientStreamIdentification;
    }

    public String getComponentName() {
        return componentName;
    }

    public String queueName() {
        return clientStreamIdentification.toString();
    }

    public String toString() {
        return clientStreamIdentification.toString();
    }

    public void enqueueQuery(SerializedQuery request, FlowControlQueues<QueryInstruction> queryQueue, long timeout,
                             boolean streaming) {
        QueryInstruction.Query query = new QueryInstruction.Query(getClientStreamIdentification(),
                                                                  getClientId(),
                                                                  request.withClient(getClientStreamId()),
                                                                  timeout,
                                                                  0L,
                                                                  streaming);
        enqueueInstruction(queryQueue, QueryInstruction.query(query));
    }

    public void enqueueCancellation(String requestId, String queryName,
                                    FlowControlQueues<QueryInstruction> queryQueue) {
        QueryInstruction.Cancel cancel = new QueryInstruction.Cancel(requestId,
                                                                     queryName,
                                                                     getClientStreamIdentification());
        enqueueInstruction(queryQueue, QueryInstruction.cancel(cancel));
    }

    public void enqueueFlowControl(String requestId, String queryName, long permits,
                                   FlowControlQueues<QueryInstruction> queryQueue) {
        QueryInstruction.FlowControl flowControl = new QueryInstruction.FlowControl(requestId,
                                                                                    queryName,
                                                                                    getClientStreamIdentification(),
                                                                                    permits);
        enqueueInstruction(queryQueue, QueryInstruction.flowControl(flowControl));
    }

    public void enqueueInstruction(FlowControlQueues<QueryInstruction> queryQueue, QueryInstruction instruction) {
        queryQueue.put(queueName(), instruction, instruction.priority());
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

    public String getClientStreamId() {
        return clientStreamIdentification.getClientStreamId();
    }

    public String getClientId() {
        return clientId;
    }
}
