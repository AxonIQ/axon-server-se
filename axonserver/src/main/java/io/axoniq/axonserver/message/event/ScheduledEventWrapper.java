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
 * @author Marc Gathier
 */
public class ScheduledEventWrapper {

    private byte[] bytes;
    private String context;

    public ScheduledEventWrapper(String context, byte[] bytes) {
        this.bytes = bytes;
        this.context = context;
    }

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
