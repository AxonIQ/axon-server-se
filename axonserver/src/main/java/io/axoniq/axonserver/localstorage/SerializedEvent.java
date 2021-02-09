/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.event.Event;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.compress.utils.IOUtils.toByteArray;

/**
 * Wrapper around an Event that keeps track of the Serialized form of the Event, to prevent unnecessary
 * (un)marshalling of Event messages.
 */
public class SerializedEvent  {

    private final byte[] serializedData;
    private volatile Event event;

    public SerializedEvent(Event event) {
        this.serializedData = event.toByteArray();
        this.event = event;
    }

    public SerializedEvent(byte[] eventFromFile) {
        this.serializedData = eventFromFile;
    }

    public SerializedEvent(InputStream event) {
        try {
            this.serializedData = toByteArray(event);
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    public InputStream asInputStream() {
        return new ByteArrayInputStream(serializedData);
    }

    public Event asEvent() {
        if (event == null) {
            try {
                event = Event.parseFrom(serializedData);
            } catch (InvalidProtocolBufferException e) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
            }
        }
        return event;
    }

    public int size() {
        return serializedData.length;
    }

    public byte[] serializedData() {
        return serializedData;
    }

    public long getAggregateSequenceNumber() {
        return asEvent().getAggregateSequenceNumber();
    }

    public byte[] getPayload() {
        return asEvent().getPayload().getData().toByteArray();
    }

    public String getIdentifier() {
        return asEvent().getMessageIdentifier();
    }

    public String getAggregateType() {
        return asEvent().getAggregateType();
    }

    public String getPayloadType() {
        return asEvent().getPayload().getType();
    }

    public String getPayloadRevision() {
        return asEvent().getPayload().getRevision();
    }

    public long getTimestamp() {
        return asEvent().getTimestamp();
    }

    public boolean isDomainEvent() {
        return ! StringUtils.isEmpty(getAggregateType());
    }

    public String getAggregateIdentifier() {
        return asEvent().getAggregateIdentifier();
    }

    public long getSequenceNumber() {
        return asEvent().getAggregateSequenceNumber();
    }

    public String getType() {
        return asEvent().getAggregateType();
    }

    public Map<String, Object> getMetaData() {
        Map<String,Object> metaData = new HashMap<>();
        asEvent().getMetaDataMap().forEach((key, value) -> metaData.put(key, asObject(value)));
        return metaData;
    }

    private Object asObject(MetaDataValue value) {
        switch (value.getDataCase()) {
            case TEXT_VALUE:
                return value.getTextValue();
            case NUMBER_VALUE:
                return value.getNumberValue();
            case BOOLEAN_VALUE:
                return value.getBooleanValue();
            case DOUBLE_VALUE:
                return value.getDoubleValue();
            case BYTES_VALUE:
                return value.getBytesValue();
        }
        return "Null";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SerializedEvent that = (SerializedEvent) o;
        return Arrays.equals(serializedData, that.serializedData);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(serializedData);
    }

    public ByteString asByteString() {
        return ByteString.copyFrom(serializedData);
    }

    public SerializedEvent asSnapshot() {
        return new SerializedEvent(Event.newBuilder(asEvent()).setSnapshot(true).build());
    }

    public boolean isSnapshot() {
        return asEvent().getSnapshot();
    }
}
