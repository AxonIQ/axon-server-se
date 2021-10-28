/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.query.QueryComplete;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.WrappedQuery;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Reads messages for a specific client from a queue and sends them to the client using gRPC.
 * Only reads messages when there are permits left.
 * @author Marc Gathier
 */
public class GrpcQueryDispatcherListener extends GrpcFlowControlledDispatcherListener<QueryProviderInbound,WrappedQuery> implements QueryRequestValidator {
    private static final Logger logger = LoggerFactory.getLogger(GrpcQueryDispatcherListener.class);
    private final QueryDispatcher queryDispatcher;

    public GrpcQueryDispatcherListener(QueryDispatcher queryDispatcher, String client, StreamObserver<QueryProviderInbound> queryProviderInboundStreamObserver, int threads) {
        super(queryDispatcher.getQueryQueue(), client, queryProviderInboundStreamObserver, threads);
        this.queryDispatcher = queryDispatcher;
    }

    @Override
    protected boolean send(WrappedQuery message) {
        if( logger.isDebugEnabled()) {
            logger.debug("Send request {}, with priority: {}", message.queryRequest(), message.priority() );
        }
        // TODO: 10/28/21 refactor WrappedQuery
        if (!message.isTerminateQuery()) {
            SerializedQuery request = validate(message, queryDispatcher, logger);
            if (request == null) return false;
            inboundStream.onNext(QueryProviderInbound.newBuilder().setQuery(request.query()).build());
        } else {
            inboundStream.onNext(QueryProviderInbound.newBuilder()
                                                     .setQueryComplete(QueryComplete.newBuilder()
                                                                                    .setRequestId(message.queryRequest()
                                                                                                         .getMessageIdentifier())
                                                                                    .setMessageId(UUID.randomUUID()
                                                                                                      .toString())
                                                                                    .build()).build());
        }
        return true;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}

