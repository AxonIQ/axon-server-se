package io.axoniq.axonserver.refactoring.transport.grpc;

import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.refactoring.messaging.api.Payload;
import org.jetbrains.annotations.NotNull;

/**
 * @author Milan Savic
 */
public class SerializedObjectMapping implements Payload {

    private final SerializedObject payload;

    public SerializedObjectMapping(SerializedObject payload) {
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
