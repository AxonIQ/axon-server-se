/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
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

import java.util.Objects;

/**
 * Basic handler for queries. Puts a query in a specific queue to send it based on its priority to the target client.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public abstract class QueryHandler {

    protected final ClientStreamIdentification clientStreamIdentification;
    private final String componentName;
    private final String clientId;

    protected QueryHandler(ClientStreamIdentification clientStreamIdentification,
                           String componentName, String clientId) {
        this.clientStreamIdentification = clientStreamIdentification;
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


    public String toString() {
        return clientStreamIdentification.toString();
    }

    /**
     * Enqueues the query for later dispatching.
     *
     * @param request   the serialized query request
     * @param timeout   how long we should wait for this query
     * @param streaming indicates whether this query is streaming results or not
     * @return the function to cancel the query, removing the request from the queue if still possible or enqueueing a
     * cancel instruction otherwise
     */
    public abstract Cancellable dispatchQuery(SerializedQuery request,
                                              long timeout,
                                              boolean streaming);


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryHandler that = (QueryHandler) o;
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

    public abstract void dispatchFlowControl(String requestId, String queryName, long permits);
}
