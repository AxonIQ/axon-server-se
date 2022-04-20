package io.axoniq.axonserver.component.version;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class ClientVersionsCacheTest {

    @Test
    public void apply() {

        ClientVersionsCache testSubject = new ClientVersionsCache();
        testSubject.on(new ClientVersionUpdate("A", "context1", "4.2.1"));
        testSubject.on(new ClientVersionUpdate("B", "context1", "4.2.2"));
        testSubject.on(new ClientVersionUpdate("C", "context2", "4.2.1"));
        testSubject.on(new ClientVersionUpdate("D", "context2", "4.2.3"));
        testSubject.on(new ClientVersionUpdate("E", "context2", "4.2.3"));

        assertEquals("4.2.1", testSubject.apply(new ClientStreamIdentification("context1", "A")));
        assertNull(testSubject.apply(new ClientStreamIdentification("A", "context2")));
        assertEquals("4.2.2", testSubject.apply(new ClientStreamIdentification("context1", "B")));
        assertEquals("4.2.1", testSubject.apply(new ClientStreamIdentification("context2", "C")));
        assertEquals("4.2.3", testSubject.apply(new ClientStreamIdentification("context2", "D")));
        assertEquals("4.2.3", testSubject.apply(new ClientStreamIdentification("context2", "E")));

        testSubject.on(new TopologyEvents.ApplicationDisconnected("context2", "componentName", "E", "test"));
        assertNull(testSubject.apply(new ClientStreamIdentification("E", "context2")));
    }
}