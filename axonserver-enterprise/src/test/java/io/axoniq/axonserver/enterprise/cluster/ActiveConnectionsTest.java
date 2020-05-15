package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import org.junit.*;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class ActiveConnectionsTest {

    private ActiveConnections testSubject = new ActiveConnections();

    @Test
    public void onConnectDisconnect() {
        RemoteConnection newConnection = mock(RemoteConnection.class);
        when(newConnection.getClusterNode()).thenReturn(new ClusterNode("name", null, null, null, null, null));
        testSubject.on(new ClusterEvents.AxonServerInstanceConnected(newConnection));

        Set<String> connectedNodes = new HashSet<>();
        for (String s : testSubject) {
            connectedNodes.add(s);
        }
        assertEquals(1, connectedNodes.size());
        assertTrue(connectedNodes.contains("name"));
        ;

        testSubject.on(new ClusterEvents.AxonServerInstanceDisconnected("name2"));
        connectedNodes = new HashSet<>();
        for (String s : testSubject) {
            connectedNodes.add(s);
        }
        assertEquals(1, connectedNodes.size());
        assertTrue(connectedNodes.contains("name"));
        ;

        testSubject.on(new ClusterEvents.AxonServerInstanceDisconnected("name"));
        connectedNodes = new HashSet<>();
        for (String s : testSubject) {
            connectedNodes.add(s);
        }
        assertEquals(0, connectedNodes.size());
    }
}