package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.command.CommandResponse;
import org.junit.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class SerializedCommandResponseTest {
    private SerializedCommandResponse testSubject;

    @Test
    public void testSerializeDeserializeConfirmation() throws IOException {
        CommandResponse commandResponse = CommandResponse.newBuilder()
                                                         .setRequestIdentifier("12345")
                                                         .build();
        testSubject = new SerializedCommandResponse(commandResponse);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        testSubject.writeTo(outputStream);

        SerializedCommandResponse parsed = (SerializedCommandResponse) SerializedCommandResponse.getDefaultInstance().getParserForType()
                                                                                                                     .parseFrom(outputStream.toByteArray());
        assertEquals("12345", parsed.getRequestIdentifier());
    }

}