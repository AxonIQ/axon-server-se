package io.axoniq.axonserver.enterprise.jpa;

import io.axoniq.axonserver.grpc.SerializedObject;

import javax.persistence.Embeddable;
import javax.persistence.Lob;

/**
 * Serialized payload for a scheduled task
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Embeddable
public class Payload {

    private String type;

    @Lob
    private byte[] data;

    public Payload() {
    }

    public Payload(String type, byte[] bytes) {
        this.type = type;
        this.data = bytes;
    }

    public Payload(SerializedObject payload) {
        this(payload.getType(), payload.getData().toByteArray());
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
