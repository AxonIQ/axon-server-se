package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.grpc.ClientContext;
import io.axoniq.axonserver.grpc.ClientIdRegistry;
import io.axoniq.axonserver.grpc.ClientIdRegistry.ConnectionType;
import io.axoniq.axonserver.grpc.DefaultClientIdRegistry;
import io.axoniq.axonserver.grpc.heartbeat.ApplicationInactivityListener.StreamCloser;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.axoniq.axonserver.grpc.ClientIdRegistry.ConnectionType.COMMAND;
import static io.axoniq.axonserver.grpc.ClientIdRegistry.ConnectionType.QUERY;
import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class ApplicationInactivityListenerTest {

    private final String context = "context";
    private final String component = "component";
    private final ClientContext client = new ClientContext("clientId", context);
    private final String commandStream = "commandStream";
    private final String queryStream = "queryStream";
    private final String platformStream = "platformStream";

    @Test
    public void testCloseStreams() {
        ClientIdRegistry registry = new DefaultClientIdRegistry();
        registry.register(commandStream, client, COMMAND);
        registry.register(queryStream, client, QUERY);
        AtomicBoolean commandStreamClosed = new AtomicBoolean();
        AtomicBoolean queryStreamClosed = new AtomicBoolean();
        Map<ConnectionType, StreamCloser> streamClosers = new HashMap<>();
        streamClosers.put(COMMAND, ((client, streamIdentification) -> commandStreamClosed.set(true)));
        streamClosers.put(QUERY, ((client, streamIdentification) -> queryStreamClosed.set(true)));
        ApplicationInactivityListener testSubject = new ApplicationInactivityListener(streamClosers, registry);
        ClientStreamIdentification platformStream = new ClientStreamIdentification(context, this.platformStream);
        testSubject.on(new TopologyEvents.ApplicationInactivityTimeout(platformStream, component, client));
        assertTrue(commandStreamClosed.get());
        assertTrue(queryStreamClosed.get());
    }

    @Test
    public void testCloseCommandStream() {
        ClientIdRegistry registry = new DefaultClientIdRegistry();
        registry.register(commandStream, client, COMMAND);
        AtomicBoolean commandStreamClosed = new AtomicBoolean();
        AtomicBoolean queryStreamClosed = new AtomicBoolean();
        Map<ConnectionType, StreamCloser> streamClosers = new HashMap<>();
        streamClosers.put(COMMAND, ((client, streamIdentification) -> commandStreamClosed.set(true)));
        streamClosers.put(QUERY, ((client, streamIdentification) -> queryStreamClosed.set(true)));
        ApplicationInactivityListener testSubject = new ApplicationInactivityListener(streamClosers, registry);
        ClientStreamIdentification platformStream = new ClientStreamIdentification(context, this.platformStream);
        testSubject.on(new TopologyEvents.ApplicationInactivityTimeout(platformStream, component, client));
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
        testSubject.on(new TopologyEvents.ApplicationInactivityTimeout(platformStream, component, client));
        assertFalse(commandStreamClosed.get());
        assertFalse(queryStreamClosed.get());
    }
}