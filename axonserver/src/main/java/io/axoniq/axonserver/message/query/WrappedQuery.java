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
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;

/**
 * Wrapper around a serialized query to use for handling messages from query queues.
 *
 * @author Marc Gathier
 */
public class WrappedQuery {

    public enum QueryType {
        QUERY, TERMINATE, FLOW_CONTROL
    }

    private final ClientStreamIdentification targetClientStreamIdentification;
    private final String targetClientId;
    private final SerializedQuery queryRequest;
    private final long timeout;
    private final long priority;
    private final QueryType queryType;
    private final long flowControl;

    public static WrappedQuery terminateQuery(String queryId, String context) {
        return new WrappedQuery(new ClientStreamIdentification(context, null),
                                null,
                                new SerializedQuery(null,
                                                    QueryRequest.newBuilder()
                                                                .setMessageIdentifier(queryId)
                                                                .build()),
                                0L,
                                QueryType.TERMINATE,
                0L);
    }

    public static WrappedQuery flowControl(String queryId, long flowControl, String context) {
        return new WrappedQuery(new ClientStreamIdentification(context, null),
                null,
                new SerializedQuery(null,
                        QueryRequest.newBuilder()
                                .setMessageIdentifier(queryId)
                                .build()),
                0L,
                QueryType.FLOW_CONTROL,
                flowControl);
    }

    public WrappedQuery(ClientStreamIdentification targetClientStreamIdentification,
                        String targetClientId, SerializedQuery queryRequest, long timeout) {
        this(targetClientStreamIdentification, targetClientId, queryRequest, timeout, QueryType.QUERY, 0L);
    }

    public WrappedQuery(ClientStreamIdentification targetClientStreamIdentification,
                        String targetClientId, SerializedQuery queryRequest, long timeout, QueryType queryType,
                        long flowControl) {
        this.targetClientStreamIdentification = targetClientStreamIdentification;
        this.targetClientId = targetClientId;
        this.queryRequest = queryRequest;
        this.timeout = timeout;
        this.priority = ProcessingInstructionHelper.priority(queryRequest.query().getProcessingInstructionsList());
        this.queryType = queryType;
        this.flowControl = flowControl;
    }

    public SerializedQuery queryRequest() {
        return queryRequest;
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

    public QueryType queryType() {
        return queryType;
    }

    public long flowControl() {
        return flowControl;
    }

    public WrappedQuery withTargetClientStreamId(String targetClientStreamId) {
        return new WrappedQuery(targetClientStreamIdentification.withClientStreamId(targetClientStreamId),
                targetClientId,
                queryRequest,
                timeout,
                queryType,
                flowControl);
    }
}
