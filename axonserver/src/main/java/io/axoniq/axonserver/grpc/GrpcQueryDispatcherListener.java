/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.query.QueryFlowControl;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryReference;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.QueryInstruction;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static java.lang.String.format;

/**
 * Reads messages for a specific client from a queue and sends them to the client using gRPC. Only reads messages when
 * there are permits left.
 *
 * @author Marc Gathier
 */
public class GrpcQueryDispatcherListener
        extends GrpcFlowControlledDispatcherListener<QueryProviderInbound, QueryInstruction>
        implements QueryRequestValidator {

    private static final Logger logger = LoggerFactory.getLogger(GrpcQueryDispatcherListener.class);
    private final QueryDispatcher queryDispatcher;

    public GrpcQueryDispatcherListener(QueryDispatcher queryDispatcher, String client,
                                       StreamObserver<QueryProviderInbound> queryProviderInboundStreamObserver,
                                       int threads) {
        super(queryDispatcher.getQueryQueue(), client, queryProviderInboundStreamObserver, threads);
        this.queryDispatcher = queryDispatcher;
    }

    @Override
    protected boolean send(QueryInstruction queryInstruction) {
        if (queryInstruction.query().isPresent()) {
            return send(queryInstruction.query().get());
        } else if (queryInstruction.cancel().isPresent()) {
            return send(queryInstruction.cancel().get());
        } else if (queryInstruction.flowControl().isPresent()) {
            return send(queryInstruction.flowControl().get());
        } else {
            throw new IllegalStateException("Unsupported queryInstruction to be sent.");
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    private boolean send(QueryInstruction.Query query) {
        debug(() -> format("Send query request %s, with priority: %d", query.queryRequest(), query.priority()));
        SerializedQuery serializedQuery = validate(query, queryDispatcher, logger);
        if (serializedQuery == null) {
            return false;
        }
        QueryRequest.Builder requestBuilder =
                serializedQuery.query()
                               .toBuilder()
                               .addProcessingInstructions(streamingProcessingInstruction(query.streaming()));
        inboundStream.onNext(QueryProviderInbound.newBuilder()
                                                 .setQuery(requestBuilder)
                                                 .build());
        return true;
    }

    private boolean send(QueryInstruction.Cancel cancel) {
        debug(() -> format("Send query cancellation %s.", cancel.requestId()));
        QueryReference queryReference = QueryReference.newBuilder()
                                                      .setRequestId(cancel.requestId())
                                                      .build();
        inboundStream.onNext(QueryProviderInbound.newBuilder()
                                                 .setQueryCancel(queryReference)
                                                 .build());
        return true;
    }

    private boolean send(QueryInstruction.FlowControl flowControl) {
        debug(() -> format("Send query flow control %s, permits %d.",
                           flowControl.requestId(),
                           flowControl.flowControl()));
        QueryReference queryReference = QueryReference.newBuilder()
                                                      .setRequestId(flowControl.requestId())
                                                      .build();
        QueryFlowControl flowControlMessage =
                QueryFlowControl.newBuilder()
                                .setQueryReference(queryReference)
                                .setPermits(flowControl.flowControl())
                                .build();
        inboundStream.onNext(QueryProviderInbound.newBuilder()
                                                 .setQueryFlowControl(flowControlMessage)
                                                 .build());
        return true;
    }

    private ProcessingInstruction streamingProcessingInstruction(boolean streaming) {
        MetaDataValue.Builder value = MetaDataValue.newBuilder()
                                                   .setBooleanValue(streaming);
        return ProcessingInstruction.newBuilder()
                                    .setKey(ProcessingKey.SERVER_SUPPORTS_STREAMING)
                                    .setValue(value)
                                    .build();
    }

    private void debug(Supplier<String> messageSupplier) {
        if (logger.isDebugEnabled()) {
            logger.debug(messageSupplier.get());
        }
    }
}

