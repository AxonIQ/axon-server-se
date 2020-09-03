/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class DirectQueryHandler extends QueryHandler<QueryProviderInbound> {

    public DirectQueryHandler(StreamObserver<QueryProviderInbound> streamObserver,
                              ClientStreamIdentification clientIdentification,
                              String componentName, String clientId) {
        super(streamObserver, clientIdentification, componentName, clientId);
    }

    @Override
    public void dispatch(SubscriptionQueryRequest query) {
        streamObserver.onNext(QueryProviderInbound.newBuilder()
                                                  .setSubscriptionQueryRequest(query)
                                                  .build());
    }
}
