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
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/**
 * Basic handler for queries. Puts a query in a specific queue to send it based on its priority to the target client.
 * @author Marc Gathier
 * @since 4.0
 */
public abstract class QueryHandler<T>  {
    private final ClientIdentification client;
    private final String componentName;
    protected final StreamObserver<T> streamObserver;

    protected QueryHandler(StreamObserver<T> streamObserver, ClientIdentification client, String componentName) {
        this.client = client;
        this.streamObserver = streamObserver;
        this.componentName = componentName;
    }

    /**
     * Directly sends a query (initial query for a subscription query to the target client)
     * @param query the query to send
     */
    public abstract void dispatch(SubscriptionQueryRequest query);

    public ClientIdentification getClient() {
        return client;
    }

    public String getComponentName() {
        return componentName;
    }

    public String queueName() {
        return client.toString();
    }

    public String toString() {
        return client.toString();
    }

    /**
     * Enqueues a query for the target client. Queries will be read from queues based on priorities.
     * @param request the query to send
     * @param queryQueue the queue holders for queries
     * @param timeout timeout of the query
     */
    public void enqueue(SerializedQuery request, FlowControlQueues<WrappedQuery> queryQueue, long timeout) {
        WrappedQuery wrappedQuery = new WrappedQuery(request.withClient(getClientId()), timeout);
        queryQueue.put(queueName(), wrappedQuery, wrappedQuery.priority());
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
        return Objects.equals(client, that.client);
    }

    @Override
    public int hashCode() {
        return Objects.hash(client);
    }

    public String getClientId() {
        return client.getClientId();
    }
}
