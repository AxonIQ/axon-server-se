package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.grpc.cluster.Entry;
import org.junit.*;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Author: marc
 */
public class InMemoryLogEntryStoreTest {
    private InMemoryLogEntryStore testSubject;

    @Before
    public void setUp() throws Exception {
        testSubject = new InMemoryLogEntryStore();
    }

    @Test
    public void appendEntry() {
        testSubject.appendEntry(asList(newEntry(1, 0), newEntry(1, 1), newEntry(1, 2)));
        assertNotNull( testSubject.getEntry(0));
        assertNotNull( testSubject.getEntry(1));
        assertNotNull( testSubject.getEntry(2));
    }

    @Test
    public void replaceEntries() {
        testSubject.appendEntry(asList(newEntry(1, 0), newEntry(1, 1), newEntry(1, 2)));
        testSubject.appendEntry(Collections.singletonList(newEntry(2, 1)));
        assertNotNull( testSubject.getEntry(0));
        assertEquals( 2, testSubject.getEntry(1).getTerm());
        assertNull( testSubject.getEntry(2));
    }

    @Test
    public void applyEntry() {
        AtomicInteger entryCounter = new AtomicInteger();
        testSubject.appendEntry(asList(newEntry(1, 0), newEntry(1, 1), newEntry(1, 2)));
        testSubject.applyEntries(e -> entryCounter.incrementAndGet());
        assertEquals(0, entryCounter.get());

        testSubject.markCommitted(1);
        testSubject.applyEntries(e -> entryCounter.incrementAndGet());
        assertEquals(2, entryCounter.get());
        assertEquals( 1, testSubject.lastAppliedIndex());
    }

    private static Entry newEntry(long term, long index) {
        return Entry.newBuilder()
                    .setTerm(term)
                    .setIndex(index)
                    .build();
    }
}