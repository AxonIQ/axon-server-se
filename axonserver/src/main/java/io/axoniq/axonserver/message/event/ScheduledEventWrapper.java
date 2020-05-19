/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

/**
 * Combines event data with the context in which the scheduled event will be applied.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class ScheduledEventWrapper {

    /**
     * The protobuf serialized event data
     */
    private byte[] bytes;
    /**
     * The name of the context
     */
    private String context;

    /**
     * Constructor.
     *
     * @param context the context where the scheduled event will be applied
     * @param bytes   the event data
     */
    public ScheduledEventWrapper(String context, byte[] bytes) {
        this.bytes = bytes;
        this.context = context;
    }

    /**
     * Default constructor for deserializing the object.
     */
    public ScheduledEventWrapper() {
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }
}
