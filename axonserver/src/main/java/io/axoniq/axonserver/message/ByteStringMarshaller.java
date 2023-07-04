/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message;

import com.google.protobuf.ByteString;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.io.InputStream;

public class ByteStringMarshaller implements MethodDescriptor.Marshaller<ByteString> {

    public static ByteStringMarshaller instance() {
        return new ByteStringMarshaller();
    }

    @Override
    public InputStream stream(ByteString bytes) {
        return bytes.newInput();
    }

    @Override
    public ByteString parse(InputStream stream) {
        try {
            return ByteString.readFrom(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
