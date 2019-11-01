package io.axoniq.axonserver.enterprise.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.enterprise.jpa.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author Marc Gathier
 */
@Component
public class JacksonTaskPayloadSerializer implements TaskPayloadSerializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Object deserialize(Payload payload) throws ClassNotFoundException, IOException {
        return objectMapper.readValue(new String(payload.getData()),
                                      objectMapper.getTypeFactory().findClass(payload.getType()));
    }

    @Override
    public Payload serialize(Object object) throws JsonProcessingException {
        return new Payload(object.getClass().getName(), objectMapper.writeValueAsString(object).getBytes());
    }
}
