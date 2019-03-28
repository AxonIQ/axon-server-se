/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;

import java.util.Map;

/**
 * Base wrapper for gRPC messages that maintains serialized data to reduce time to write to stream.
 *
 * @author Marc Gathier
 */
public abstract class SerializedMessage<T extends Message> extends AbstractMessage {
    protected abstract T wrapped();

    @Override
    public Descriptors.Descriptor getDescriptorForType() {
        return wrapped().getDescriptorForType();
    }

    @Override
    public Map<Descriptors.FieldDescriptor, Object> getAllFields() {
        return wrapped().getAllFields();
    }

    @Override
    public boolean hasField(Descriptors.FieldDescriptor fieldDescriptor) {
        return wrapped().hasField(fieldDescriptor);
    }

    @Override
    public Object getField(Descriptors.FieldDescriptor fieldDescriptor) {
        return wrapped().getField(fieldDescriptor);
    }

    @Override
    public int getRepeatedFieldCount(Descriptors.FieldDescriptor fieldDescriptor) {
        return wrapped().getRepeatedFieldCount(fieldDescriptor);
    }

    @Override
    public Object getRepeatedField(Descriptors.FieldDescriptor fieldDescriptor, int i) {
        return wrapped().getRepeatedField(fieldDescriptor, i);
    }

    @Override
    public UnknownFieldSet getUnknownFields() {
        return wrapped().getUnknownFields();
    }

}
