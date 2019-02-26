package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.junit.*;

import java.io.IOException;
import java.util.Collections;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
public class InMemoryEntryIteratorTest {

    @Test
    public void testPreviousWithoutCallNext() {
        InMemoryEntryIterator iterator = new InMemoryEntryIterator(new InMemoryLogEntryStore("MyName"), 1);
        TermIndex previous = iterator.previous();
        assertNull(previous);
    }

    @Test
    public void testPreviousStartingFrom1() throws IOException {
        InMemoryLogEntryStore store = new InMemoryLogEntryStore("MyName");
        Entry entry = Entry.newBuilder().setIndex(1).setTerm(1).build();
        store.appendEntry(Collections.singletonList(entry));
        InMemoryEntryIterator iterator = new InMemoryEntryIterator(store, 1);
        assertTrue(iterator.hasNext());
        assertEquals(entry, iterator.next());
        assertEquals(0, iterator.previous().getIndex());
        assertEquals(0, iterator.previous().getTerm());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyStore() {
        InMemoryEntryIterator iterator = new InMemoryEntryIterator(new InMemoryLogEntryStore("MyName"), 1);
        assertFalse(iterator.hasNext());
        iterator.next();
    }
}