package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.grpc.cluster.Entry;
import org.junit.*;
import org.springframework.data.util.CloseableIterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static io.axoniq.axonserver.cluster.replication.EntryFactory.newEntry;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class MultiSegmentIteratorTest {

    private MultiSegmentIterator testSubject;

    private CloseableIterator<Entry> getSegmentIterator(Long index, TreeMap<Long, List<Entry>> segmentsMap) {
        NavigableMap<Long, List<Entry>> headMap = segmentsMap.headMap(index, true);
        if (headMap.isEmpty()) {
            return null;
        }

        Map.Entry<Long, List<Entry>> segment = headMap.lastEntry();
        if (segment.getKey() + segment.getValue().size() - 1 < index) {
            return null;
        }

        List<Entry> remainingEntries = segment.getValue().subList((int) (index - segment.getKey()),
                                                                  segment.getValue().size());
        return new CloseableIterator<Entry>() {
            Iterator<Entry> wrapped = remainingEntries.iterator();

            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                return wrapped.hasNext();
            }

            @Override
            public Entry next() {
                return wrapped.next();
            }
        };
    }

    @Test
    public void next() {
        TreeMap<Long, List<Entry>> segmentsMap = new TreeMap<>();
        segmentsMap.put(5L, Arrays.asList(newEntry(1, 5), newEntry(1, 6), newEntry(1, 7), newEntry(1, 8)));
        segmentsMap.put(9L, Arrays.asList(newEntry(1, 9), newEntry(1, 10), newEntry(1, 11), newEntry(1, 12)));
        testSubject = new MultiSegmentIterator(index -> getSegmentIterator(index, segmentsMap), () -> 12L, 7);

        Entry lastRead = null;
        long expectedIndex = 7;
        while (testSubject.hasNext()) {
            lastRead = testSubject.next();
            assertEquals(expectedIndex, lastRead.getIndex());
            expectedIndex++;
        }

        assertNotNull(lastRead);
        assertEquals(lastRead.getIndex(), 12);
        assertNotNull(testSubject.previous());
        assertEquals(testSubject.previous().getIndex(), 11);
    }

    @Test
    public void nextFromFirst() {
        TreeMap<Long, List<Entry>> segmentsMap = new TreeMap<>();
        segmentsMap.put(5L, Arrays.asList(newEntry(1, 5), newEntry(1, 6), newEntry(1, 7), newEntry(1, 8)));
        segmentsMap.put(9L, Arrays.asList(newEntry(1, 9), newEntry(1, 10), newEntry(1, 11), newEntry(1, 12)));
        testSubject = new MultiSegmentIterator(index -> getSegmentIterator(index, segmentsMap), () -> 12L, 5);

        Entry lastRead = null;
        while (testSubject.hasNext()) {
            lastRead = testSubject.next();
        }

        assertNotNull(lastRead);
        assertEquals(lastRead.getIndex(), 12);
        assertNotNull(testSubject.previous());
        assertEquals(testSubject.previous().getIndex(), 11);
    }
}