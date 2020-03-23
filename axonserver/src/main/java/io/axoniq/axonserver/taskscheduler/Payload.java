/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.taskscheduler;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.SerializedObject;

import javax.persistence.Embeddable;
import javax.persistence.Lob;

/**
 * Serialized payload for a scheduled task
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Embeddable
public class Payload {

    private String type;

    @Lob
    private byte[] data;

    public Payload() {
    }

    public Payload(String type, byte[] bytes) {
        this.type = type;
        this.data = bytes;
    }

    public Payload(SerializedObject payload) {
        this(payload.getType(), payload.getData().toByteArray());
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public SerializedObject asSerializedObject() {
        return SerializedObject.newBuilder()
                               .setType(type)
                               .setData(ByteString.copyFrom(data))
                               .build();
    }
}
