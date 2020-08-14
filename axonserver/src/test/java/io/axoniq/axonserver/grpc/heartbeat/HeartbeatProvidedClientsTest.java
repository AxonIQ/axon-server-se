package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.component.instance.ClientIdentifications;
import io.axoniq.axonserver.component.version.BackwardsCompatibleVersion;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.junit.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link HeartbeatProvidedClients}
 *
 * @author Sara Pellegrini
 */
public class HeartbeatProvidedClientsTest {

    @Test
    public void iterator() {
        ClientIdentifications clients = () -> Arrays.asList((ClientStreamIdentification) new ClientStreamIdentification(
                                                                    "context",
                                                                    "A"),
                                                            new ClientStreamIdentification("context", "B"),
                                                            new ClientStreamIdentification("context", "C"),
                                                            new ClientStreamIdentification("context", "D"),
                                                            new ClientStreamIdentification("context", "E"),
                                                            new ClientStreamIdentification("context", "F"),
                                                            new ClientStreamIdentification("context", "G"),
                                                            new ClientStreamIdentification("context", "H")
        ).iterator();
        Map<String, String> versionSupplier = new HashMap<>();
        versionSupplier.put("A", "3.8.1");
        versionSupplier.put("B", "4.1.8");
        versionSupplier.put("C", "4.2.0");
        versionSupplier.put("D", "4.2.2");
        versionSupplier.put("E", "4.3.5");
        versionSupplier.put("F", "4.4.7");
        versionSupplier.put("G", "5.1.3");
        HeartbeatProvidedClients testSubjects = new HeartbeatProvidedClients(
                clients,
                clientId -> new BackwardsCompatibleVersion(versionSupplier.get(clientId.getClientStreamId())));

        Iterator<ClientStreamIdentification> iterator = testSubjects.iterator();
        assertEquals("D", iterator.next().getClientStreamId());
        assertEquals("E", iterator.next().getClientStreamId());
        assertEquals("F", iterator.next().getClientStreamId());
        assertEquals("G", iterator.next().getClientStreamId());
        assertFalse(iterator.hasNext());
    }
}