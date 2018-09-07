package io.axoniq.axonserver.localstorage;

import io.axoniq.axondb.Event;
import io.axoniq.platform.SerializedObject;
import org.junit.*;
import org.junit.rules.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class InputStreamAggregateReaderTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static TestInputStreamStorageContainer testStorageContainer;
    private AggregateReader testSubject;

    @BeforeClass
    public static void init() throws Exception {
        testStorageContainer = new TestInputStreamStorageContainer(tempFolder.getRoot());
        testStorageContainer.createDummyEvents(1000, 100);

        SnapshotWriteStorage snapshotWriteStorage = new SnapshotWriteStorage(testStorageContainer.getTransactionManager(testStorageContainer.getSnapshotManagerChain()));
        snapshotWriteStorage.store(Event.newBuilder().setAggregateIdentifier("55").setAggregateSequenceNumber(75)
                                        .setAggregateType("Snapshot")
                                        .setPayload(SerializedObject
                                                            .newBuilder().build()).build());

    }

    @Before
    public void setUp() {
        testSubject = new AggregateReader(testStorageContainer.getDatafileManagerChain(), new SnapshotReader(testStorageContainer.getSnapshotManagerChain()));
    }

    @Test
    public void readEventsFromOldSegment() {
        AtomicLong sequenceNumber = new AtomicLong();
        testSubject.readEvents("1", true, 0,
                               event -> sequenceNumber.set(event.getAggregateSequenceNumber()));
        assertEquals(99, sequenceNumber.intValue());
    }

    @Test
    public void readEventsFromCurrentSegment() {
        AtomicLong sequenceNumber = new AtomicLong();
        testSubject.readEvents("999", true, 0,
                               event -> sequenceNumber.set(event.getAggregateSequenceNumber()));
        assertEquals(99, sequenceNumber.intValue());
    }

    @Test
    public void readEventsWithSnapshot() {
        List<Event> events = new ArrayList<>();
        testSubject.readEvents("55", true, 0, event -> {
            events.add(event);
        });

        assertEquals(25, events.size());
        assertEquals("Snapshot", events.get(0).getAggregateType());
    }

    @Test
    public void readEventsWithSnapshotBeforeMin() {
        List<Event> events = new ArrayList<>();
        testSubject.readEvents("55", true, 90, event -> {
            events.add(event);
        });

        assertEquals(10, events.size());
        assertEquals("Demo", events.get(0).getAggregateType());
    }

}