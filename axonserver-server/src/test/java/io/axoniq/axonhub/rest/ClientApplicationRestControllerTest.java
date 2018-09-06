package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.component.instance.Client;
import io.axoniq.axonhub.component.instance.Clients;
import io.axoniq.axonhub.component.instance.FakeClient;
import org.junit.*;

import java.util.Iterator;

import static io.axoniq.axonhub.context.ContextController.DEFAULT;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 27/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ClientApplicationRestControllerTest {

    @Test
    public void getComponentInstances() {
        Clients clients = () -> asList( (Client) new FakeClient("clientA",DEFAULT, true),
                                        new FakeClient("clientB",DEFAULT, false),
                                        new FakeClient("clientC",DEFAULT, false)).iterator();

        ClientApplicationRestController controller = new ClientApplicationRestController(clients);
        Iterator iterator = controller.getComponentInstances("test", DEFAULT).iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }
}