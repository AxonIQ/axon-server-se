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

import java.util.Objects;

/**
 * Basic handler for queries. Puts a query in a specific queue to send it based on its priority to the target client.
 * @author Marc Gathier
 * @since 4.0
 */
public abstract class QueryHandler {

    private final ClientStreamIdentification clientStreamIdentification;
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
     * Dispatches a query for the target client.
     * // Queries will be read from queues based on priorities.
     * @param request the query to send
     * @param timeout timeout of the query
     */
    public abstract void dispatch(SerializedQuery request, long timeout);

    public ClientStreamIdentification getClientStreamIdentification() {
        return clientStreamIdentification;
    }

    public String getComponentName() {
        return componentName;
    }

    public String toString() {
        return clientStreamIdentification.toString();
    }

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

    public String getClientStreamId() {
        return clientStreamIdentification.getClientStreamId();
    }

    public String getClientId() {
        return clientId;
    }

    public boolean isDirect() {
        return true;
    }

    public String getMessagingServerName() {
        return null;
    }

    public void close() {
    }
}
