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
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;

/**
 * @author Marc Gathier
 */
public class FlowControlledQueryHandler extends QueryHandler{

    private final FlowControlQueues<WrappedQuery> flowControlQueues;
    private final QueryCache queryCache;

    public FlowControlledQueryHandler(FlowControlQueues<WrappedQuery> flowControlQueues,
                                      ClientStreamIdentification clientIdentification,
                                      String componentName, String clientId,
                                      QueryCache queryCache) {
        super( clientIdentification, componentName, clientId);
        this.flowControlQueues = flowControlQueues;
        this.queryCache = queryCache;
    }

    @Override
    public void dispatch(SubscriptionQueryRequest query) {
        WrappedQuery wrappedQuery = new WrappedQuery(getClientStreamIdentification(),
                                                     getClientId(),
                                                     query, 1000000);
        flowControlQueues.put(getClientStreamIdentification().toString(), wrappedQuery);
    }

    @Override
    public void dispatch(SerializedQuery request, long timeout) {
        WrappedQuery wrappedQuery = new WrappedQuery(getClientStreamIdentification(),
                                              getClientId(),
                                              request, timeout);
        flowControlQueues.put(getClientStreamIdentification().toString(), wrappedQuery, wrappedQuery.priority()< 0);
    }

    @Override
    public void close() {
        flowControlQueues.drain(getClientStreamIdentification().toString(),
                            query -> {
                                if (queryCache != null && query.queryRequest() != null) {
                                        queryCache.error(ErrorCode.CONNECTION_TO_HANDLER_LOST,
                                                         getClientStreamId(),
                                                         query.queryRequest());
                                }
                            });
    }
}
