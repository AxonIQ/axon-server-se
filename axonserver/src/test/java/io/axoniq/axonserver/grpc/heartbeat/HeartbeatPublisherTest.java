package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.component.instance.ClientIdentifications;
import io.axoniq.axonserver.grpc.control.Heartbeat;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.message.ClientIdentification;
import org.junit.*;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction.newBuilder;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link HeartbeatPublisher}.
 *
 * @author Sara Pellegrini
 */
public class HeartbeatPublisherTest {

    @Test
    public void publish() {
        ClientIdentifications clients = () -> Arrays.asList(new ClientIdentification("context", "A"),
                                                            new ClientIdentification("context", "B"))
                                                    .iterator();
        Map<String, PlatformOutboundInstruction> receivedHeartbeat = new ConcurrentHashMap<>();
        PlatformOutboundInstruction instruction = newBuilder().setHeartbeat(Heartbeat.newBuilder().build()).build();

        HeartbeatPublisher testSubject = new HeartbeatPublisher(clients, receivedHeartbeat::put);
        testSubject.publish(instruction);

        assertEquals(2, receivedHeartbeat.size());
        assertTrue(receivedHeartbeat.containsKey("A"));
        assertTrue(receivedHeartbeat.containsKey("B"));
        assertFalse(receivedHeartbeat.containsKey("C"));

        assertEquals(instruction, receivedHeartbeat.get("A"));
        assertEquals(instruction, receivedHeartbeat.get("B"));
    }
}