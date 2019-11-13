package io.axoniq.axonserver.enterprise.taskscheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.enterprise.jpa.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Serializer to serialize task payloads in JSON format.
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class JacksonTaskPayloadSerializer implements TaskPayloadSerializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Tries to deserialize the payload for a task. Throws {@link RuntimeException} when an error occurs.
     *
     * @param payload the payload to deserialize
     * @return the deserialized object
     */
    @Override
    public Object deserialize(Payload payload) {
        try {
            return objectMapper.readValue(new String(payload.getData()),
                                          objectMapper.getTypeFactory().findClass(payload.getType()));
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Deserializing payload failed", e);
        }
    }

    /**
     * Tries to serialize the payload for a task. Throws {@link RuntimeException} when an error occurs.
     * @param object the object to serialize
     * @return the serialized object
     */
    @Override
    public Payload serialize(Object object) {
        try {
            return new Payload(object.getClass().getName(), objectMapper.writeValueAsString(object).getBytes());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Serializing payload failed", e);
        }
    }
}
