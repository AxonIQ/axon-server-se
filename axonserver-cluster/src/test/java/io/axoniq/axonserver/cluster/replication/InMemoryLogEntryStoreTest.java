package io.axoniq.axonserver.cluster.replication;

import org.junit.*;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.cluster.TestUtils.newEntry;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class InMemoryLogEntryStoreTest {
    private InMemoryLogEntryStore testSubject;

    @Before
    public void setUp() throws Exception {
        testSubject = new InMemoryLogEntryStore("Test");
    }

    @Test
    public void appendEntry() throws IOException {
        testSubject.appendEntry(asList(newEntry(1, 1), newEntry(1, 2), newEntry(1, 3)));
        assertNotNull( testSubject.getEntry(1));
        assertNotNull( testSubject.getEntry(2));
        assertNotNull( testSubject.getEntry(3));
    }

    @Test
    public void replaceEntries() throws IOException {
        testSubject.appendEntry(asList(newEntry(1, 1), newEntry(1, 2), newEntry(1, 3)));
        testSubject.appendEntry(Collections.singletonList(newEntry(2, 2)));
        assertNotNull( testSubject.getEntry(1));
        assertEquals( 2, testSubject.getEntry(2).getTerm());
        assertNull( testSubject.getEntry(3));
    }

    @Test
    public void clearOlderThan() throws IOException {
        testSubject.appendEntry(asList(newEntry(1, 1),
                                       newEntry(1, 2),
                                       newEntry(1, 3),
                                       newEntry(1, 4)));
        testSubject.clearOlderThan(0, TimeUnit.MILLISECONDS, () -> 4);
        assertFalse(testSubject.contains(1,1));
        assertFalse(testSubject.contains(2,1));
        assertTrue(testSubject.contains(3,1));
        assertTrue(testSubject.contains(4,1));
    }
}