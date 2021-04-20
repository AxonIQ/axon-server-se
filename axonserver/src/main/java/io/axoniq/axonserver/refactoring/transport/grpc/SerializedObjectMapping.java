package io.axoniq.axonserver.refactoring.transport.grpc;

import io.axoniq.axonserver.refactoring.messaging.api.SerializedObject;

/**
 * @author Milan Savic
 */
public class SerializedObjectMapping implements SerializedObject {

    private final io.axoniq.axonserver.grpc.SerializedObject payload;

    public SerializedObjectMapping(io.axoniq.axonserver.grpc.SerializedObject payload) {
        this.payload = payload;
    }

    @Override
    public String type() {
        return payload.getType();
    }

    @Override
    public String revision() {
        return payload.getRevision();
    }

    @Override
    public byte[] data() {
        return payload.getData().toByteArray();
    }
}
