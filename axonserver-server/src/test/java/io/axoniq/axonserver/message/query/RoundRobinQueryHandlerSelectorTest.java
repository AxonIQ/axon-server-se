package io.axoniq.axonserver.message.query;

import org.junit.*;

import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class RoundRobinQueryHandlerSelectorTest {
    private RoundRobinQueryHandlerSelector testSubject;

    @Before
    public void setUp() throws Exception {
        testSubject = new RoundRobinQueryHandlerSelector();
    }

    @Test
    public void select() {
        NavigableSet<String> clients = new TreeSet<>();
        clients.add("client1");
        clients.add("client2");
        String selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client1", selected);
        selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client2", selected);
        selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client1", selected);
    }

    @Test
    public void selectWithoutLast() {
        NavigableSet<String> clients = new TreeSet<>();
        clients.add("client1");
        String selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client1", selected);
        clients = new TreeSet<>();
        clients.add("client2");
        selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client2", selected);
        selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client2", selected);
    }

}