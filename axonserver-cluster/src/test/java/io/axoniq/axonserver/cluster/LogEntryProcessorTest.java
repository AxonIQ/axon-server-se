package io.axoniq.axonserver.cluster;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class LogEntryProcessorTest {
    private LogEntryProcessor testSubject;

    @Before
    public void setup() {
        testSubject = new LogEntryProcessor(new InMemoryProcessorStore());

    }

    @Test
    public void applyEntries() {



        AtomicInteger entryCounter = new AtomicInteger();
        Function<Long, EntryIterator> entryIteratorFunction = (seq) -> new EntryIterator() {
            List<Entry> entryList = new ArrayList<>(asList(newEntry(1, 1), newEntry(1, 2), newEntry(1, 3)));

            @Override
            public boolean hasNext() {
                return !entryList.isEmpty();
            }

            @Override
            public Entry next() {
                return entryList.remove(0);
            }

            @Override
            public TermIndex previous() {
                return null;
            }

        };
        testSubject.applyEntries(entryIteratorFunction, e -> entryCounter.incrementAndGet());
        assertEquals(0, entryCounter.get());

        testSubject.markCommitted(2,1);
        testSubject.applyEntries(entryIteratorFunction, e -> entryCounter.incrementAndGet());
        assertEquals(2, entryCounter.get());
    }

    @Test
    public void markCommitted() {
    }

    private static Entry newEntry(long term, long index) {
        SerializedObject serializedObject = SerializedObject.newBuilder().setData(ByteString
                                                                                          .copyFromUtf8("Test: + index"))
                                                            .setType("Test")
                                                            .build();
        return Entry.newBuilder()
                    .setTerm(term)
                    .setIndex(index)
                    .setSerializedObject(serializedObject)
                    .build();
    }
}