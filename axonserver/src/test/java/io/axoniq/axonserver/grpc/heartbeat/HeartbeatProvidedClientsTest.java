package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.component.instance.FakeClient;
import io.axoniq.axonserver.component.version.BackwardsCompatibleVersion;
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
        Clients clients = () -> Arrays.asList((Client) new FakeClient("A", false),
                                              new FakeClient("B", false),
                                              new FakeClient("C", false),
                                              new FakeClient("D", false),
                                              new FakeClient("E", false),
                                              new FakeClient("F", false),
                                              new FakeClient("G", false),
                                              new FakeClient("H", false)
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
                clientId -> new BackwardsCompatibleVersion(versionSupplier.get(clientId.getClient())));

        Iterator<Client> iterator = testSubjects.iterator();
        assertEquals("D", iterator.next().name());
        assertEquals("E", iterator.next().name());
        assertEquals("F", iterator.next().name());
        assertEquals("G", iterator.next().name());
        assertFalse(iterator.hasNext());
    }
}