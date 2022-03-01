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
import io.axoniq.axonserver.message.ClientStreamIdentification;

import java.util.Optional;

/**
 * A query instruction. Contains all necessary information for the query instruction to be executed on the client side.
 *
 * @author Marc Gathier
 * @author Milan Savic
 */
public class QueryInstruction {

    private final Query query;
    private final Cancel cancel;
    private final FlowControl flowControl;

    /**
     * Creates an instruction to execute the given {@code query}.
     *
     * @param query the query to be executed
     * @return a freshly created query instruction
     * @see Query
     */
    public static QueryInstruction query(Query query) {
        return new QueryInstruction(query, null, null);
    }

    /**
     * Creates an instruction to cancel the query.
     *
     * @param cancel necessary information about the query to be cancelled
     * @return a freshly created query instruction
     * @see Cancel
     */
    public static QueryInstruction cancel(Cancel cancel) {
        return new QueryInstruction(null, cancel, null);
    }

    /**
     * Creates an instruction to request more responses from the query handler.
     *
     * @param flowControl necessary information for requesting more responses
     * @return a freshly created query instruction
     * @see FlowControl
     */
    public static QueryInstruction flowControl(FlowControl flowControl) {
        return new QueryInstruction(null, null, flowControl);
    }

    private QueryInstruction(Query query, Cancel cancel, FlowControl flowControl) {
        this.query = query;
        this.cancel = cancel;
        this.flowControl = flowControl;
    }

    /**
     * @return the query, if any, to be dispatched.
     */
    public Optional<Query> query() {
        return Optional.ofNullable(query);
    }

    /**
     * @return the cancellation, if any, of the query.
     */
    public Optional<Cancel> cancel() {
        return Optional.ofNullable(cancel);
    }

    /**
     * @return the flow control, if any, for the query.
     */
    public Optional<FlowControl> flowControl() {
        return Optional.ofNullable(flowControl);
    }

    /**
     * @return the identifier of the query request.
     */
    public String requestId() {
        if (query().isPresent()) {
            return query().get().queryRequest().getMessageIdentifier();
        } else if (cancel().isPresent()) {
            return cancel().get().requestId();
        } else if (flowControl().isPresent()) {
            return flowControl().get().requestId();
        }
        return null;
    }

    /**
     * @return the context of the query.
     */
    public String context() {
        if (query().isPresent()) {
            return query().get().context();
        } else if (cancel().isPresent()) {
            return cancel().get().context();
        } else if (flowControl().isPresent()) {
            return flowControl().get().context();
        }
        return null;
    }

    /**
     * @return the priority of the instruction.
     */
    public long priority() {
        if (query().isPresent()) {
            return query().get().priority();
        }
        return 0L;
    }

    @Override
    public String toString() {
        return "QueryInstruction{" +
                "query=" + query +
                ", cancel=" + cancel +
                ", flowControl=" + flowControl +
                '}';
    }

    /**
     * The query to be dispatched.
     */
    public static class Query {

        private final ClientStreamIdentification targetClientStreamIdentification;
        private final String targetClientId;
        private final SerializedQuery queryRequest;
        private final long timeout;
        private final long priority;
        private final boolean streaming;

        public Query(ClientStreamIdentification targetClientStreamIdentification, String targetClientId,
                     SerializedQuery queryRequest, long timeout, long priority, boolean streaming) {
            this.targetClientStreamIdentification = targetClientStreamIdentification;
            this.targetClientId = targetClientId;
            this.queryRequest = queryRequest;
            this.timeout = timeout;
            this.priority = priority;
            this.streaming = streaming;
        }

        public ClientStreamIdentification targetClientStreamIdentification() {
            return targetClientStreamIdentification;
        }

        public String targetClientId() {
            return targetClientId;
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

        public boolean streaming() {
            return streaming;
        }

        public String targetClientStreamId() {
            return targetClientStreamIdentification().getClientStreamId();
        }

        public String context() {
            return targetClientStreamIdentification().getContext();
        }

        @Override
        public String toString() {
            return "Query{" +
                    "targetClientStreamIdentification=" + targetClientStreamIdentification +
                    ", targetClientId='" + targetClientId + '\'' +
                    ", queryRequest=" + queryRequest +
                    ", timeout=" + timeout +
                    ", priority=" + priority +
                    ", streaming=" + streaming +
                    '}';
        }
    }

    /**
     * Cancellation of the query.
     */
    public static class Cancel {

        private final String requestId;
        private final String queryName;
        private final ClientStreamIdentification targetClientStreamIdentification;

        public Cancel(String requestId,
                      String queryName,
                      ClientStreamIdentification targetClientStreamIdentification) {
            this.requestId = requestId;
            this.queryName = queryName;
            this.targetClientStreamIdentification = targetClientStreamIdentification;
        }

        public String requestId() {
            return requestId;
        }

        public String queryName() {
            return queryName;
        }

        public ClientStreamIdentification targetClientStreamIdentification() {
            return targetClientStreamIdentification;
        }

        public String targetClientStreamId() {
            return targetClientStreamIdentification().getClientStreamId();
        }

        public String context() {
            return targetClientStreamIdentification().getContext();
        }

        @Override
        public String toString() {
            return "Cancel{" +
                    "requestId='" + requestId + '\'' +
                    ", queryName='" + queryName + '\'' +
                    ", targetClientStreamIdentification=" + targetClientStreamIdentification +
                    '}';
        }
    }

    /**
     * Flow control for the query.
     */
    public static class FlowControl {

        private final String requestId;
        private final String queryName;
        private final ClientStreamIdentification targetClientStreamIdentification;
        private final long flowControl;

        public FlowControl(String requestId,
                           String queryName,
                           ClientStreamIdentification targetClientStreamIdentification,
                           long flowControl) {
            this.requestId = requestId;
            this.queryName = queryName;
            this.targetClientStreamIdentification = targetClientStreamIdentification;
            this.flowControl = flowControl;
        }

        public String requestId() {
            return requestId;
        }

        public String queryName() {
            return queryName;
        }

        public ClientStreamIdentification targetClientStreamIdentification() {
            return targetClientStreamIdentification;
        }

        public long flowControl() {
            return flowControl;
        }

        public String targetClientStreamId() {
            return targetClientStreamIdentification().getClientStreamId();
        }

        public String context() {
            return targetClientStreamIdentification().getContext();
        }

        @Override
        public String toString() {
            return "FlowControl{" +
                    "requestId='" + requestId + '\'' +
                    ", queryName='" + queryName + '\'' +
                    ", targetClientStreamIdentification=" + targetClientStreamIdentification +
                    ", flowControl=" + flowControl +
                    '}';
        }
    }
}
