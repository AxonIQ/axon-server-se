package io.axoniq.axonserver.cluster.replication.file;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.junit.*;
import org.junit.rules.*;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class FileSegmentLogEntryStoreTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private FileSegmentLogEntryStore testSubject;
    private PrimaryEventStore primary;

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

    private static Entry newEntry(long term, long index) {
        SerializedObject serializedObject = SerializedObject.newBuilder().setData(ByteString.copyFromUtf8("Test: + index"))
                                                            .setType("Test")
                                                            .build();
        return Entry.newBuilder()
                    .setTerm(term)
                    .setIndex(index)
                    .setSerializedObject(serializedObject)
                    .build();
    }

    @Test
    public void testIterator() {
        primary = PrimaryEventStoreFactory.create("node5");
        testSubject = new FileSegmentLogEntryStore("Test", primary);
        EntryIterator iterator = testSubject.createIterator(900);
        while( iterator.hasNext()) {
            Entry entry =iterator.next();
            System.out.println(entry.getIndex());
        }

    }
}