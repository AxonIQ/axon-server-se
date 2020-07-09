package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.component.instance.FakeClient;
import io.axoniq.axonserver.grpc.control.Heartbeat;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
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
        Clients clients = () -> Arrays.asList((Client) new FakeClient("A", false),
                                              new FakeClient("B", false)).iterator();
        Map<String, PlatformOutboundInstruction> receivedHeartbeat = new ConcurrentHashMap<>();
        PlatformOutboundInstruction instruction = newBuilder().setHeartbeat(Heartbeat.newBuilder().build()).build();

        HeartbeatPublisher testSubject = new HeartbeatPublisher(clients, (context, client, i) -> receivedHeartbeat.put(client, i));
        testSubject.publish(instruction);

        assertEquals(2, receivedHeartbeat.size());
        assertTrue(receivedHeartbeat.containsKey("A"));
        assertTrue(receivedHeartbeat.containsKey("B"));
        assertFalse(receivedHeartbeat.containsKey("C"));

        assertEquals(instruction, receivedHeartbeat.get("A"));
        assertEquals(instruction, receivedHeartbeat.get("B"));
    }
}