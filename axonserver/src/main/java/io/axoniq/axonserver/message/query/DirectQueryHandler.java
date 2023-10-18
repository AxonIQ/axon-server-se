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
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.Cancellable;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Marc Gathier
 */
public class DirectQueryHandler extends QueryHandler {

    private final Logger logger = LoggerFactory.getLogger(DirectQueryHandler.class);
    private final StreamObserver<QueryProviderInbound> streamToHandler;
    private final FlowControlQueues<QueryInstruction> queryInstructionFlowControlQueues;


    public DirectQueryHandler(StreamObserver<QueryProviderInbound> streamToHandler,
                              FlowControlQueues<QueryInstruction> queryInstructionFlowControlQueues,
                              ClientStreamIdentification clientIdentification,
                              String componentName, String clientId) {
        super(clientIdentification, componentName, clientId);
        this.streamToHandler = streamToHandler;
        this.queryInstructionFlowControlQueues = queryInstructionFlowControlQueues;
    }

    @Override
    public void dispatch(SubscriptionQueryRequest query) {
        streamToHandler.onNext(QueryProviderInbound.newBuilder()
                                                   .setSubscriptionQueryRequest(query)
                                                   .build());
    }

    @Override
    public Cancellable dispatchQuery(SerializedQuery request,
                                     long timeout,
                                     boolean streaming) {
        logger.trace("Enqueueing query request instruction {} for target client {}.",
                     request.getMessageIdentifier(), clientStreamIdentification);
        QueryInstruction.Query query = new QueryInstruction.Query(getClientStreamIdentification(),
                                                                  getClientId(),
                                                                  request.withClient(getClientStreamId()),
                                                                  timeout,
                                                                  0L,
                                                                  streaming);
        Cancellable cancellable = enqueueInstruction(QueryInstruction.query(query));
        return () -> {
            logger.debug("Cancelling the query request {} for target client {}.",
                         request.getMessageIdentifier(),
                         clientStreamIdentification);
            if (!cancellable.cancel()) {
                logger.trace("Enqueueing cancel instruction to target client {} for query {}.",
                             clientStreamIdentification, request.getMessageIdentifier());
                enqueueCancellation(request.getMessageIdentifier(), request.query().getQuery());
            } else {
                logger.trace("Query request {} removed from sending queue to target client {}.",
                             request.getMessageIdentifier(),
                             clientStreamIdentification);
            }
            return true;
        };
    }

    /**
     * Enqueues cancellation for the query with given {@code requestId} and {@code queryName}.
     *
     * @param requestId the identifier of the query request
     * @param queryName the name of the query
     */
    private void enqueueCancellation(String requestId, String queryName) {
        QueryInstruction.Cancel cancel = new QueryInstruction.Cancel(requestId,
                                                                     queryName,
                                                                     getClientStreamIdentification());
        enqueueInstruction(QueryInstruction.cancel(cancel));
    }


    /**
     * Enqueues flow control of {@code permits} for the query with given {@code requestId} and {@code queryName}.
     *
     * @param requestId the identifier of the query request
     * @param queryName the name of the query
     * @param permits   the permits - how many results we are demanindg from this handler to produce
     */
    public void dispatchFlowControl(String requestId, String queryName, long permits) {
        QueryInstruction.FlowControl flowControl = new QueryInstruction.FlowControl(requestId,
                                                                                    queryName,
                                                                                    getClientStreamIdentification(),
                                                                                    permits);
        enqueueInstruction(QueryInstruction.flowControl(flowControl));
    }

    /**
     * Enqueues the given {@code instruction} to the given {@code queryQueue}.
     *
     * @param instruction the query instruction
     * @return the function to remove the instruction from the queue
     */
    private Cancellable enqueueInstruction(QueryInstruction instruction) {
        return queryInstructionFlowControlQueues.put(queueName(), instruction, instruction.priority());
    }

    /**
     * Returns the name of the query that this handler can receive
     *
     * @return the name of the query that this handler can receive
     */
    public String queueName() {
        return clientStreamIdentification.getClientStreamId();
    }
}
