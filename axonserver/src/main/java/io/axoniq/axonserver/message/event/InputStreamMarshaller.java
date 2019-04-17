/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.grpc.MethodDescriptor;

import java.io.BufferedInputStream;
import java.io.InputStream;

/**
 * @author Marc Gathier
 */
public class InputStreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {

    public static InputStreamMarshaller inputStreamMarshaller() {
        return new InputStreamMarshaller();
    }

    @Override
    public InputStream stream(InputStream inputStream) {
        return inputStream;
    }

    @Override
    public InputStream parse(InputStream stream) {
        if (stream.markSupported()) {
            return stream;
        } else {
            return new BufferedInputStream(stream);
        }
    }

}
