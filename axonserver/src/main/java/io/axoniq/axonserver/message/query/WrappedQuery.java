/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;

/**
 * Wrapper around a serialized query to use for handling messages from query queues.
 *
 * @author Marc Gathier
 */
public class WrappedQuery {

    private final ClientStreamIdentification targetClientStreamIdentification;
    private final String targetClientId;
    private final SerializedQuery queryRequest;
    private final SubscriptionQueryRequest subscriptionQueryRequest;
    private final long timeout;
    private final long priority;

    public WrappedQuery(ClientStreamIdentification targetClientStreamIdentification,
                        String targetClientId, SerializedQuery queryRequest, long timeout) {
        this.targetClientStreamIdentification = targetClientStreamIdentification;
        this.targetClientId = targetClientId;
        this.queryRequest = queryRequest;
        this.timeout = timeout;
        this.priority = ProcessingInstructionHelper.priority(queryRequest.query().getProcessingInstructionsList());
        this.subscriptionQueryRequest = null;
    }
    public WrappedQuery(ClientStreamIdentification targetClientStreamIdentification,
                        String targetClientId, SubscriptionQueryRequest subscriptionQueryRequest) {
        this.targetClientStreamIdentification = targetClientStreamIdentification;
        this.targetClientId = targetClientId;
        this.queryRequest = null;
        this.timeout = Long.MAX_VALUE;
        this.priority = 0;
        this.subscriptionQueryRequest = subscriptionQueryRequest;
    }

    public QueryProviderInbound request() {
        if (subscriptionQueryRequest != null) {
            return QueryProviderInbound.newBuilder()
                                       .setSubscriptionQueryRequest(subscriptionQueryRequest)
                                       .build();
        }

        return QueryProviderInbound.newBuilder().setQuery(queryRequest.query()).build();
    }

    public long timeout() {
        return timeout;
    }

    public long priority() {
        return priority;
    }

    public String targetClientId() {
        return targetClientId;
    }

    public String targetClientStreamId() {
        return targetClientStreamIdentification.getClientStreamId();
    }

    public String context() {
        return targetClientStreamIdentification.getContext();
    }

    public SerializedQuery queryRequest() {
        return queryRequest;
    }
}
