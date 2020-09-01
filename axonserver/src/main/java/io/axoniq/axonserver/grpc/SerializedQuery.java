/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.query.QueryRequest;

/**
 * Wrapper around {@link QueryRequest} to reduce serialization/deserialization.
 *
 * @author Marc Gathier
 */
public class SerializedQuery {

    private final String clientStreamId;
    private final String context;
    private volatile QueryRequest query;
    private final byte[] serializedData;

    public SerializedQuery(String context, QueryRequest query) {
        this(context, null, query);
    }

    public SerializedQuery(String context, String clientStreamId, QueryRequest query) {
        this.context = context;
        this.clientStreamId = clientStreamId;
        this.serializedData = query.toByteArray();
        this.query = query;
    }

    public SerializedQuery(String context, String clientStreamId, byte[] readByteArray) {
        this.context = context;
        this.clientStreamId = clientStreamId;
        serializedData = readByteArray;
    }


    /**
     * Creates a copy of the object with client set to {@code newClient}.
     *
     * @param targetClientStreamId the client stream id to dispatch the query to
     * @return a copy of the serialized query
     */
    public SerializedQuery withClient(String targetClientStreamId) {
        return new SerializedQuery(context, targetClientStreamId, serializedData);
    }

    public QueryRequest query() {
        if (query == null) {
            try {
                query = QueryRequest.parseFrom(serializedData);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
        return query;
    }

    public String context() {
        return context;
    }

    public String clientStreamId() {
        return clientStreamId;
    }

    public ByteString toByteString() {
        return ByteString.copyFrom(serializedData);
    }

    public SerializedQuery withTimeout(long remainingTime) {
        int timeoutIndex = -1;
        QueryRequest request = query();
        for (int i = 0; i < request.getProcessingInstructionsList().size(); i++) {
            if (ProcessingKey.TIMEOUT.equals(request.getProcessingInstructions(i).getKey())) {
                timeoutIndex = i;
                break;
            }
        }


        if (timeoutIndex >= 0) {
            request = QueryRequest.newBuilder(request)
                                  .removeProcessingInstructions(timeoutIndex)
                                  .addProcessingInstructions(ProcessingInstructionHelper.timeout(remainingTime))
                                  .build();
        } else {
            request = QueryRequest.newBuilder(request)
                                  .addProcessingInstructions(ProcessingInstructionHelper.timeout(remainingTime))
                                  .build();
        }
        return new SerializedQuery(context, clientStreamId, request);
    }

    public String getMessageIdentifier() {
        return query().getMessageIdentifier();
    }
}
