/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.grpc.MethodDescriptor;

import java.io.InputStream;

/**
 * @author Marc Gathier
 */
public class SerializedEventMarshaller implements MethodDescriptor.Marshaller<SerializedEvent> {

    public static SerializedEventMarshaller serializedEventMarshaller() {
        return new SerializedEventMarshaller();
    }

    @Override
    public InputStream stream(SerializedEvent value) {
        return value.asInputStream();
    }

    @Override
    public SerializedEvent parse(InputStream stream) {
        return new SerializedEvent(stream);
    }
}
