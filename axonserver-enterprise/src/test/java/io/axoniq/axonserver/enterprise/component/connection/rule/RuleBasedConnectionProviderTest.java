package io.axoniq.axonserver.enterprise.component.connection.rule;

import io.axoniq.axonserver.message.ClientIdentification;
import org.junit.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link RuleBasedConnectionProvider}
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public class RuleBasedConnectionProviderTest {

    private Map<String, Double> values;
    private RuleBasedConnectionProvider testSubject;


    @Before
    public void setUp() throws Exception {
        values = new HashMap<>();
        values.put("ServerA", 5d);
        values.put("ServerB", 3d);
        values.put("ServerC", 8d);
        testSubject = new RuleBasedConnectionProvider((client, server) -> () -> values.getOrDefault(server, 0d));
    }

    @Test
    public void testBestMatch() {
        String bestMatch = testSubject.bestMatch(new ClientIdentification("context", "client"), values.keySet());
        assertEquals("ServerC", bestMatch);
    }

    @Test
    public void testBestMatchOneNodeNotActive() {
        Collection<String> nodes = new LinkedList<>(values.keySet());
        nodes.remove("ServerC");
        String bestMatch = testSubject.bestMatch(new ClientIdentification("context", "client"), nodes);
        assertEquals("ServerA", bestMatch);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullClient() {
        String bestMatch = testSubject.bestMatch(null, values.keySet());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullServer() {
        String bestMatch = testSubject.bestMatch(new ClientIdentification("context", "client"), null);
    }

    @Test
    public void testNoActiveServers() {
        Collection<String> nodes = new LinkedList<>();
        String bestMatch = testSubject.bestMatch(new ClientIdentification("context", "client"), nodes);
        assertNull(bestMatch);
    }
}