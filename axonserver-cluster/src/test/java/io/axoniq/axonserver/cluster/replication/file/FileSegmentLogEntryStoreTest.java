package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.replication.EntryFactory;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.junit.*;
import org.junit.rules.*;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class FileSegmentLogEntryStoreTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private FileSegmentLogEntryStore testSubject;
    private PrimaryLogEntryStore primary;

    @Before
    public void setUp() throws Exception {
        primary = PrimaryEventStoreFactory.create(tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());
        testSubject = new FileSegmentLogEntryStore("Test", primary);
    }

    @After
    public void complete() {
        System.out.println( tempFolder.getRoot());
        primary.cleanup(0);
    }


    @Test
    public void appendEntry() throws IOException {
        testSubject.appendEntry(asList(EntryFactory.newEntry(1, 1), EntryFactory.newEntry(1, 2), EntryFactory
                .newEntry(1, 3)));
        assertNotNull( testSubject.getEntry(1));
        assertNotNull( testSubject.getEntry(2));
        assertNotNull( testSubject.getEntry(3));
    }

    @Test
    public void replaceEntries() throws IOException {
        testSubject.appendEntry(asList(EntryFactory.newEntry(1, 1), EntryFactory.newEntry(1, 2), EntryFactory
                .newEntry(1, 3)));
        testSubject.appendEntry(Collections.singletonList(EntryFactory.newEntry(2, 2)));
        assertNotNull( testSubject.getEntry(1));
        assertEquals(1, testSubject.getEntry(1).getTerm());
        assertEquals( 2, testSubject.getEntry(2).getTerm());
        assertNull( testSubject.getEntry(3));
        testSubject.appendEntry(Collections.singletonList(EntryFactory.newEntry(3, 2)));
        assertEquals(1, testSubject.getEntry(1).getTerm());
        assertEquals(3, testSubject.getEntry(2).getTerm());
    }

    @Test
    public void testIterator() {
        primary = PrimaryEventStoreFactory.create(tempFolder.getRoot().getAbsolutePath() + "/node5");
        testSubject = new FileSegmentLogEntryStore("Test", primary);
        EntryIterator iterator = testSubject.createIterator(900);
        while( iterator.hasNext()) {
            Entry entry =iterator.next();
            System.out.println(entry.getIndex());
        }

    }
}