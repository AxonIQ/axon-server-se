package io.axoniq.axonserver.enterprise.component.connection.rule;

import io.axoniq.axonserver.message.ClientIdentification;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link TagsMatch}
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public class TagsMatchTest {

    private Map<String, Map<String, String>> clusterTags;
    private Map<ClientIdentification, Map<String, String>> clientsTags;
    private TagsMatch testSubject;

    @Before
    public void setUp() throws Exception {
        clusterTags = new HashMap<>();
        Map<String, String> nodeA = clusterTags.computeIfAbsent("nodeA", n -> new HashMap<>());
        nodeA.put("city", "Milan");
        nodeA.put("region", "Europe");
        Map<String, String> nodeB = clusterTags.computeIfAbsent("nodeB", n -> new HashMap<>());
        nodeB.put("city", "NewYork");
        nodeB.put("region", "USA");
        Map<String, String> nodeC = clusterTags.computeIfAbsent("nodeC", n -> new HashMap<>());
        nodeC.put("city", "Amsterdam");
        clientsTags = new HashMap<>();
        Map<String, String> client1 = clientsTags.computeIfAbsent(new ClientIdentification("default", "client1"),
                                                                  n -> new HashMap<>());
        client1.put("city", "Belgrade");
        client1.put("region", "Europe");
        Map<String, String> client2 = clientsTags.computeIfAbsent(new ClientIdentification("test", "client2"),
                                                                  n -> new HashMap<>());
        client2.put("city", "Milan");
        client2.put("region", "Europe");
        Map<String, String> client3 = clientsTags.computeIfAbsent(new ClientIdentification("default", "client3"),
                                                                  n -> new HashMap<>());
        client3.put("city", "Amsterdam");
        client3.put("region", "Europe");

        testSubject = new TagsMatch(node -> clusterTags.get(node), client -> clientsTags.get(client));
    }

    @Test
    public void testNoMatch() {
        ConnectionValue value = testSubject.apply(new ClientIdentification("test", "client2"), "nodeB");
        assertEquals(0, value.weight(), 0);
    }

    @Test
    public void testOneMatch() {
        ConnectionValue value = testSubject.apply(new ClientIdentification("default", "client3"), "nodeA");
        assertEquals(1, value.weight(), 0);
    }

    @Test
    public void testMissingTag() {
        ConnectionValue value = testSubject.apply(new ClientIdentification("default", "client3"), "nodeC");
        assertEquals(1, value.weight(), 0);
    }

    @Test
    public void testMissingClient() {
        ConnectionValue value = testSubject.apply(new ClientIdentification("default", "client4"), "nodeC");
        assertEquals(0, value.weight(), 0);
    }

    @Test
    public void testMissingServer() {
        ConnectionValue value = testSubject.apply(new ClientIdentification("default", "client3"), "nodeD");
        assertEquals(0, value.weight(), 0);
    }
}