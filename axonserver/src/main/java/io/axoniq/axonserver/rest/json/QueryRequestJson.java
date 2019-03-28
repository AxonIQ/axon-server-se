/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author Marc Gathier
 */
public class QueryRequestJson {
    private String messageIdentifier;
    private String query;
    private long timestamp;
    private SerializedObjectJson payload;
    private SerializedObjectJson responseType;
    private long nrOfResults = -1;
    private long timeout = 15000;
    private long priority = 0;
    private MetaDataJson metaData = new MetaDataJson();

    public QueryRequest asQueryRequest() {
        QueryRequest.Builder builder = QueryRequest.newBuilder()
                                                   .setQuery(query)
                                                   .setMessageIdentifier(StringUtils.getOrDefault(messageIdentifier,
                                                                                                  UUID.randomUUID()
                                                                                                      .toString()))
                                                   .setTimestamp(timestamp);

        if( responseType != null) {
            builder.setResponseType(responseType.asSerializedObject());
        }

        if( payload != null) {
            builder.setPayload(payload.asSerializedObject());
        }

        return builder.putAllMetaData(metaData.asMetaDataValueMap())
               .addAllProcessingInstructions(processingInstructions())
               .build();
    }

    private Iterable<? extends ProcessingInstruction> processingInstructions() {
        List<ProcessingInstruction> processingInstructionList = new ArrayList<>();
        processingInstructionList.add(ProcessingInstructionHelper.timeout(timeout));
        processingInstructionList.add(ProcessingInstructionHelper.priority(priority));
        processingInstructionList.add(ProcessingInstructionHelper.numberOfResults(nrOfResults));
        return processingInstructionList;
    }

    public String getMessageIdentifier() {
        return messageIdentifier;
    }

    public void setMessageIdentifier(String messageIdentifier) {
        this.messageIdentifier = messageIdentifier;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public SerializedObjectJson getPayload() {
        return payload;
    }

    public void setPayload(SerializedObjectJson payload) {
        this.payload = payload;
    }

    public SerializedObjectJson getResponseType() {
        return responseType;
    }

    public void setResponseType(SerializedObjectJson responseType) {
        this.responseType = responseType;
    }

    public long getNrOfResults() {
        return nrOfResults;
    }

    public void setNrOfResults(long nrOfResults) {
        this.nrOfResults = nrOfResults;
    }

    public long getTimeout() {
        return timeout;
    }

    public MetaDataJson getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaDataJson metaData) {
        this.metaData = metaData;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public long getPriority() {
        return priority;
    }

    public void setPriority(long priority) {
        this.priority = priority;
    }
}
