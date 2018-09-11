package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.component.instance.FakeClient;
import org.junit.*;

import java.util.Iterator;

import static io.axoniq.axonserver.topology.Topology.DEFAULT_CONTEXT;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 27/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ClientApplicationRestControllerTest {

    @Test
    public void getComponentInstances() {
        Clients clients = () -> asList( (Client) new FakeClient("clientA",DEFAULT_CONTEXT, true),
                                        new FakeClient("clientB",DEFAULT_CONTEXT, false),
                                        new FakeClient("clientC",DEFAULT_CONTEXT, false)).iterator();

        ClientApplicationRestController controller = new ClientApplicationRestController(clients);
        Iterator iterator = controller.getComponentInstances("test", DEFAULT_CONTEXT).iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }
}