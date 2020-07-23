package io.axoniq.axonserver.enterprise.storage.file.xref;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.file.IndexEntries;
import io.axoniq.axonserver.localstorage.file.IndexEntry;
import io.axoniq.axonserver.localstorage.file.SegmentAndPosition;
import io.axoniq.axonserver.localstorage.file.StorageProperties;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.rules.*;

import java.io.IOException;
import java.util.SortedMap;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class JumpSkipIndexManagerTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private JumpSkipIndexManager testSubject;

    @Before
    public void setUp() throws IOException {
        temporaryFolder.newFolder(Topology.DEFAULT_CONTEXT);
        SystemInfoProvider systemInfoProvider = new SystemInfoProvider() {
        };
        StorageProperties storageProperties = new StorageProperties(systemInfoProvider);
        storageProperties.setMaxIndexesInMemory(3);
        storageProperties.setStorage(temporaryFolder.getRoot().getAbsolutePath());
        storageProperties.setUseMmapIndex(true);
        storageProperties.setForceCleanMmapIndex(true);

        testSubject = new JumpSkipIndexManager(Topology.DEFAULT_CONTEXT,
                                               storageProperties,
                                               EventType.EVENT,
                                               new MeterFactory(new SimpleMeterRegistry(),
                                                                new DefaultMetricCollector()));
        testSubject.init();
    }

    @After
    public void tearDown() {
        testSubject.cleanup(true);
    }

    @Test
    public void lookupAggregateFromActiveSegments() {
        String aggregateId = "AGG_O";
        testSubject.addToActiveSegment(0, aggregateId, new IndexEntry(0, 0, 0));
        testSubject.addToActiveSegment(0, aggregateId, new IndexEntry(1, 100, 1));
        testSubject.addToActiveSegment(2, aggregateId, new IndexEntry(2, 0, 2));

        SortedMap<Long, IndexEntries> result = testSubject
                .lookupAggregate(aggregateId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, 0);
        assertEquals(2, result.size());
        long first = result.firstKey();
        assertEquals(0, first);
        assertEquals(2, result.get(first).size());
        long last = result.lastKey();
        assertEquals(2, last);
        assertEquals(1, result.get(last).size());
    }

    @Test
    public void lookupAggregateFromActiveAndCompletedSegments() {
        String aggregateId = "AGG_O";
        testSubject.addToActiveSegment(0, aggregateId, new IndexEntry(0, 0, 0));
        testSubject.addToActiveSegment(0, aggregateId, new IndexEntry(1, 100, 1));
        testSubject.addToActiveSegment(2, aggregateId, new IndexEntry(2, 0, 2));
        testSubject.complete(0);
        SortedMap<Long, IndexEntries> result = testSubject
                .lookupAggregate(aggregateId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, 0);
        assertEquals(2, result.size());
        long first = result.firstKey();
        assertEquals(0, first);
        assertEquals(2, result.get(first).size());
        long last = result.lastKey();
        assertEquals(2, last);
        assertEquals(1, result.get(last).size());
    }

    @Test
    public void lookupAggregateFromCompletedSegmentsOnly() {
        String aggregateId = "AGG_O";
        String anotherAggregateId = "AGG_1";
        testSubject.addToActiveSegment(0, aggregateId, new IndexEntry(0, 0, 0));
        testSubject.addToActiveSegment(0, aggregateId, new IndexEntry(1, 100, 1));
        testSubject.addToActiveSegment(2, aggregateId, new IndexEntry(2, 0, 2));
        testSubject.addToActiveSegment(3, anotherAggregateId, new IndexEntry(0, 0, 3));
        testSubject.complete(0);
        testSubject.complete(2);
        SortedMap<Long, IndexEntries> result = testSubject
                .lookupAggregate(aggregateId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, 0);
        assertEquals(2, result.size());
        long first = result.firstKey();
        assertEquals(0, first);
        assertEquals(2, result.get(first).size());
        long last = result.lastKey();
        assertEquals(2, last);
        assertEquals(1, result.get(last).size());
    }

    @Test
    public void lastEventFromActive() {
        String aggregateId = "AGG_O";
        testSubject.addToActiveSegment(0, aggregateId, new IndexEntry(0, 0, 0));
        SegmentAndPosition result = testSubject.lastEvent(aggregateId, 0);
        assertEquals(0, result.getSegment());
        assertEquals(0, result.getPosition());
    }

    @Test
    public void lastEventFromIndex() {
        String aggregateId = "AGG_O";
        String anotherAggregateId = "AGG_1";
        testSubject.addToActiveSegment(0, aggregateId, new IndexEntry(0, 0, 0));
        testSubject.addToActiveSegment(1, anotherAggregateId, new IndexEntry(0, 100, 1));
        testSubject.complete(0);
        SegmentAndPosition result = testSubject.lastEvent(aggregateId, 0);
        assertEquals(0, result.getSegment());
        assertEquals(0, result.getPosition());
    }

    @Test
    public void lastEventMinSequenceNumberNotFound() {
        String aggregateId = "AGG_O";
        String anotherAggregateId = "AGG_1";
        testSubject.addToActiveSegment(0, aggregateId, new IndexEntry(0, 0, 0));
        testSubject.addToActiveSegment(1, anotherAggregateId, new IndexEntry(0, 100, 1));
        testSubject.complete(0);
        assertNull(testSubject.lastEvent(aggregateId, 1));
    }

    @Test
    public void validIndex() {
        testSubject.complete(0);
        assertTrue(testSubject.validIndex(0));
        assertFalse(testSubject.validIndex(1));
    }
}