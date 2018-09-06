package io.axoniq.axonhub.component.instance;

import org.junit.*;

import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 16/05/2018.
 * sara.pellegrini@gmail.com
 */
public class ClientsNamesTest {

    @Test
    public void iterator() {
        Clients clients = () -> asList((Client) new FakeClient("A", false),
                                       new FakeClient("B", false)).iterator();
        Iterator<String> iterator = new ClientsNames(clients).iterator();
        assertTrue(iterator.hasNext());
        assertEquals("A", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("B", iterator.next());
        assertFalse(iterator.hasNext());
    }
}