/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.util.StringUtils;

import java.util.Map;

/**
 * @author Marc Gathier
 */
public class WrappedEvent implements ProcessedEvent {

    private final Event event;
    private final byte[] dataForWrite;

    public WrappedEvent(Event event, EventTransformer eventTransformer) {
        this.event = event;
        this.dataForWrite = eventTransformer.toStorage(event.toByteArray());
    }

    @Override
    public int getSerializedSize() {
        return dataForWrite.length;
    }

    @Override
    public byte[] toByteArray() {
        return dataForWrite;
    }

    @Override
    public String getAggregateIdentifier() {
        return event.getAggregateIdentifier();
    }

    @Override
    public long getAggregateSequenceNumber() {
        return event.getAggregateSequenceNumber();
    }

    @Override
    public String getMessageIdentifier() {
        return event.getMessageIdentifier();
    }

    @Override
    public byte[] getPayloadBytes() {
        return event.getPayload().getData().toByteArray();
    }

    @Override
    public String getPayloadRevision() {
        return event.getPayload().getRevision();
    }

    @Override
    public String getPayloadType() {
        return event.getPayload().getType();
    }

    @Override
    public long getTimestamp() {
        return event.getTimestamp();
    }

    @Override
    public String getAggregateType() {
        return event.getAggregateType();
    }

    @Override
    public Map<String, MetaDataValue> getMetaData() {
        return event.getMetaDataMap();
    }

    @Override
    public boolean isDomainEvent() {
        return !StringUtils.isEmpty(event.getAggregateType());
    }

}
