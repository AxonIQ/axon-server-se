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
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class DirectQueryHandler extends QueryHandler {

    private final StreamObserver<QueryProviderInbound> streamObserver;
    private final FlowControlQueues<WrappedQuery> queryQueue;

    public DirectQueryHandler(StreamObserver<QueryProviderInbound> streamObserver,
                              FlowControlQueues<WrappedQuery> queryQueue,
                              ClientIdentification clientIdentification, String componentName) {
        super(clientIdentification, componentName);
        this.streamObserver = streamObserver;
        this.queryQueue = queryQueue;
    }

    @Override
    public void dispatch(SerializedQuery request, long timeout) {
        queryQueue.put(getClientId(), new WrappedQuery(request.withClient(getClientId()), timeout));
    }


    @Override
    public void dispatch(SubscriptionQueryRequest query) {
        streamObserver.onNext(QueryProviderInbound.newBuilder()
                                                  .setSubscriptionQueryRequest(query)
                                                  .build());
    }
}
