package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.ClientStreamIdentification;
import io.axoniq.axonserver.refactoring.configuration.TopologyEvents;
import io.axoniq.axonserver.refactoring.transport.ClientIdRegistry;
import io.axoniq.axonserver.refactoring.transport.ClientIdRegistry.ConnectionType;
import io.axoniq.axonserver.refactoring.transport.DefaultClientIdRegistry;
import io.axoniq.axonserver.refactoring.transport.heartbeat.ApplicationInactivityListener;
import io.axoniq.axonserver.refactoring.transport.heartbeat.ApplicationInactivityListener.StreamCloser;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.axoniq.axonserver.refactoring.transport.ClientIdRegistry.ConnectionType.COMMAND;
import static io.axoniq.axonserver.refactoring.transport.ClientIdRegistry.ConnectionType.QUERY;
import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class ApplicationInactivityListenerTest {

    private final String context = "context";
    private final String component = "component";
    private final String clientId = "myClient";
    private final String commandStream = "commandStream";
    private final String queryStream = "queryStream";
    private final String platformStream = "platformStream";

    @Test
    public void testCloseStreams() {
        ClientIdRegistry registry = new DefaultClientIdRegistry();
        registry.register(commandStream, clientId, COMMAND);
        registry.register(queryStream, clientId, QUERY);
        AtomicBoolean commandStreamClosed = new AtomicBoolean();
        AtomicBoolean queryStreamClosed = new AtomicBoolean();
        Map<ConnectionType, StreamCloser> streamClosers = new HashMap<>();
        streamClosers.put(COMMAND, ((client, streamIdentification) -> commandStreamClosed.set(true)));
        streamClosers.put(QUERY, ((client, streamIdentification) -> queryStreamClosed.set(true)));
        ApplicationInactivityListener testSubject = new ApplicationInactivityListener(streamClosers, registry);
        ClientStreamIdentification platformStream = new ClientStreamIdentification(context, this.platformStream);
        testSubject.on(new TopologyEvents.ApplicationInactivityTimeout(platformStream, component, clientId));
        assertTrue(commandStreamClosed.get());
        assertTrue(queryStreamClosed.get());
    }

    @Test
    public void testCloseCommandStream() {
        ClientIdRegistry registry = new DefaultClientIdRegistry();
        registry.register(commandStream, clientId, COMMAND);
        AtomicBoolean commandStreamClosed = new AtomicBoolean();
        AtomicBoolean queryStreamClosed = new AtomicBoolean();
        Map<ConnectionType, StreamCloser> streamClosers = new HashMap<>();
        streamClosers.put(COMMAND, ((client, streamIdentification) -> commandStreamClosed.set(true)));
        streamClosers.put(QUERY, ((client, streamIdentification) -> queryStreamClosed.set(true)));
        ApplicationInactivityListener testSubject = new ApplicationInactivityListener(streamClosers, registry);
        ClientStreamIdentification platformStream = new ClientStreamIdentification(context, this.platformStream);
        testSubject.on(new TopologyEvents.ApplicationInactivityTimeout(platformStream, component, clientId));
        assertTrue(commandStreamClosed.get());
        assertFalse(queryStreamClosed.get());
    }

    @Test
    public void testCloseNoStream() {
        ClientIdRegistry registry = new DefaultClientIdRegistry();
        AtomicBoolean commandStreamClosed = new AtomicBoolean();
        AtomicBoolean queryStreamClosed = new AtomicBoolean();
        Map<ConnectionType, StreamCloser> streamClosers = new HashMap<>();
        streamClosers.put(COMMAND, ((client, streamIdentification) -> commandStreamClosed.set(true)));
        streamClosers.put(QUERY, ((client, streamIdentification) -> queryStreamClosed.set(true)));
        ApplicationInactivityListener testSubject = new ApplicationInactivityListener(streamClosers, registry);
        ClientStreamIdentification platformStream = new ClientStreamIdentification(context, this.platformStream);
        testSubject.on(new TopologyEvents.ApplicationInactivityTimeout(platformStream, component, clientId));
        assertFalse(commandStreamClosed.get());
        assertFalse(queryStreamClosed.get());
    }
}