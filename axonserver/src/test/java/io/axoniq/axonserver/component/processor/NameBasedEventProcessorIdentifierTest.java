package io.axoniq.axonserver.component.processor;

import org.junit.*;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link NameBasedEventProcessorIdentifier}
 *
 * @author Sara Pellegrini
 */
public class NameBasedEventProcessorIdentifierTest {

    @Test
    public void testEquals() {
        assertEquals(new NameBasedEventProcessorIdentifier("myName"),
                     new NameBasedEventProcessorIdentifier("myName"));

        assertNotEquals(new NameBasedEventProcessorIdentifier("myName"),
                        new NameBasedEventProcessorIdentifier("anotherName"));

        assertNotEquals(new NameBasedEventProcessorIdentifier("myName"),
                        (EventProcessorIdentifier) () -> "myName");
    }
}