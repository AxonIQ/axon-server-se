package io.axoniq.axonserver.component.instance;

import org.junit.*;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link ClientContextProvider}
 *
 * @author Sara Pellegrini
 */
public class ClientContextProviderTest {

    private final List<Client> clients = asList(new GenericClient("client1",
                                                                  "component1",
                                                                  "context1",
                                                                  "node1"),
                                                new GenericClient("client2",
                                                                  "component2",
                                                                  "context2",
                                                                  "node2"));

    private final ClientContextProvider testSubject = new ClientContextProvider(clients);

    @Test
    public void testGetContext() {
        assertEquals("context1", testSubject.apply("client1"));
        assertEquals("context2", testSubject.apply("client2"));
    }

    @Test(expected = RuntimeException.class)
    public void testClientNotFound() {
        testSubject.apply("client3");
    }
}