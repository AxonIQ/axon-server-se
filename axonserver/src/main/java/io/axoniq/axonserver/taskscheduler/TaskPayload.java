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
 * Serialized payload for a scheduled task.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Embeddable
public class TaskPayload {

    private String type;

    @Lob
    private byte[] data;

    /**
     * Constructor used by JPA.
     */
    public TaskPayload() {
    }

    /**
     * Constructor used to create an initialized payload.
     *
     * @param type the type of the payload
     * @param data the serialized data for the payload
     */
    public TaskPayload(String type, byte[] data) {
        this.type = type;
        this.data = data;
    }

    /**
     * Constructor that initializes a payload from a {@link SerializedObject}.
     *
     * @param payload serialized object containing a payload
     */
    public TaskPayload(SerializedObject payload) {
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

    /**
     * Creates a {@link SerializedObject} representation of the payload.
     *
     * @return SerializedObject representation of the payload
     */
    public SerializedObject asSerializedObject() {
        return SerializedObject.newBuilder()
                               .setType(type)
                               .setData(ByteString.copyFrom(data))
                               .build();
    }
}
