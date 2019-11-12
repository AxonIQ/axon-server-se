package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.enterprise.jpa.Payload;

/**
 * Defines the interface for serializing/deserializing task payloads.
 * @author Marc Gathier
 * @since 4.3
 */
public interface TaskPayloadSerializer {

    /**
     * De-serializes the payload to a Java object. Throws an exception if there is a problem de-serializing the data.
     *
     * @param payload the serialized payload
     * @return the java object
     */
    Object deserialize(Payload payload);

    /**
     * Serializes a Java object to a Payload object. Throws an exception if there is a problem serializing the data.
     *
     * @param object the java object
     * @return the serialized payload
     */
    Payload serialize(Object object);
}
