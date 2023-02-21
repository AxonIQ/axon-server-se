package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.TransformationAction;

public class ProtoTransformationEntry implements TransformationEntry {

    private final long sequence;
    private final TransformationAction payload;

    public ProtoTransformationEntry(long sequence, TransformationAction payload) {
        this.sequence = sequence;
        this.payload = payload;
    }

    @Override
    public long sequence() {
        return sequence;
    }

    @Override
    public byte[] payload() {
        return payload.toByteArray();
    }

    @Override
    public byte version() {
        return 0;
    }
}
