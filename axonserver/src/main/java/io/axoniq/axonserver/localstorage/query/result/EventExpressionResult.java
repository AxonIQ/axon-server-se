/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.result;

import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marc Gathier
 */
public class EventExpressionResult implements AbstractMapExpressionResult {
    private final EventWithToken event;

    public static final List<String> COLUMN_NAMES = Collections.unmodifiableList(Arrays.asList("token", "eventIdentifier","aggregateIdentifier", "aggregateSequenceNumber", "aggregateType", "payloadType"
                                                                   , "payloadRevision" , "payloadData" , "timestamp", "metaData"));

    public EventExpressionResult(EventWithToken event) {
        this.event = event;
    }

    @Override
    public Iterable<String> getColumnNames() {
        return COLUMN_NAMES;
    }

    @Override
    public Object getValue() {
        return event;
    }

    @Override
    public int compareTo(ExpressionResult o) {
        return 0;
    }

    @Override
    public ExpressionResult getByIdentifier(String identifier) {
        switch (identifier) {
            case "eventIdentifier":
                return new StringExpressionResult(event.getEvent().getMessageIdentifier());
            case "aggregateIdentifier":
                return new StringExpressionResult(event.getEvent().getAggregateIdentifier());
            case "aggregateSequenceNumber":
                return new NumericExpressionResult(event.getEvent().getAggregateSequenceNumber());
            case "aggregateType":
                return new StringExpressionResult(event.getEvent().getAggregateType());
            case "payloadType":
                return new StringExpressionResult(event.getEvent().getPayload().getType());
            case "payloadData":
                return new StringExpressionResult(event.getEvent().getPayload().getData().toStringUtf8());
            case "payloadRevision":
                return new StringExpressionResult(event.getEvent().getPayload().getRevision());
            case "metaData":
                Map<String, MetaDataValue> stringObjectMap = event.getEvent().getMetaDataMap();
                Map<String, ExpressionResult> expressionMap = new HashMap<>();
                stringObjectMap.forEach((k,v) -> expressionMap.put(k, toExpression(v)));
                return new MapExpressionResult(expressionMap);
            case "timestamp":
                return new TimestampExpressionResult(event.getEvent().getTimestamp());
            case "token":
                return new NumericExpressionResult(event.getToken());
            default:
                throw new RuntimeException("Invalid identifier: " + identifier);
        }
    }

    @NotNull
    private ExpressionResult toExpression(MetaDataValue s) {
        switch (s.getDataCase()) {
            case TEXT_VALUE:
                return new StringExpressionResult(s.getTextValue());
            case NUMBER_VALUE:
                return new NumericExpressionResult(s.getNumberValue());
            case BOOLEAN_VALUE:
                return BooleanExpressionResult.forValue(s.getBooleanValue());
            case DOUBLE_VALUE:
                return new NumericExpressionResult(s.getDoubleValue());
            case BYTES_VALUE:
                break;
            case DATA_NOT_SET:
                break;
        }
        return NullExpressionResult.INSTANCE;
    }

    @Override
    public boolean isNull() {
        return event == null;
    }

    @Override
    public boolean isNonNull() {
        return event != null;
    }

}
