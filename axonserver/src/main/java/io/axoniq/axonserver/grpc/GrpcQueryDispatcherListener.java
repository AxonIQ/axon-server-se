/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.WrappedQuery;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads messages for a specific client from a queue and sends them to the client using gRPC.
 * Only reads messages when there are permits left.
 * @author Marc Gathier
 */
public class GrpcQueryDispatcherListener extends GrpcFlowControlledDispatcherListener<QueryProviderInbound,WrappedQuery> implements QueryRequestValidator {
    private static final Logger logger = LoggerFactory.getLogger(GrpcQueryDispatcherListener.class);
    private final QueryDispatcher queryDispatcher;

    public GrpcQueryDispatcherListener(FlowControlQueues<WrappedQuery> flowControlQueues,
                                       String client,
                                       StreamObserver<QueryProviderInbound> queryProviderInboundStreamObserver,
                                       int threads,
                                       QueryDispatcher queryDispatcher) {
        super(flowControlQueues, client, queryProviderInboundStreamObserver, threads);
        this.queryDispatcher = queryDispatcher;
    }

    @Override
    protected boolean send(WrappedQuery message) {
        WrappedQuery request = validate(message, logger);
        if( request == null) {
            queryDispatcher.removeFromCache(message.targetClientId(), message.queryRequest().getMessageIdentifier());
            return false;
        }
        inboundStream.onNext(request.request());
        return true;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}

