package io.axoniq.axonserver.enterprise.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.axoniq.axonserver.enterprise.jpa.Payload;

import java.io.IOException;

/**
 * @author Marc Gathier
 */
public interface TaskPayloadSerializer {

    Object deserialize(Payload payload) throws ClassNotFoundException, IOException;

    Payload serialize(Object object) throws JsonProcessingException;
}
