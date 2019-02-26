package io.axoniq.axonserver.grpc;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;

import java.util.Map;

/**
 * Author: marc
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
