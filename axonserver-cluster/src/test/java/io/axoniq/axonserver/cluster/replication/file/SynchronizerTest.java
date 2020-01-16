package io.axoniq.axonserver.cluster.replication.file;

import org.junit.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class SynchronizerTest {

    private Synchronizer synchronizer;
    private Set<Long> completedSegments = new HashSet<>();

    @Before
    public void setUp() throws Exception {
        StorageProperties storageProperies = new StorageProperties();
        storageProperies.setSyncInterval(1);
        synchronizer = new Synchronizer("sample", storageProperies, w -> {
            completedSegments.add(w.segment);
        });
    }

    @Test
    public void cleanupOnShutdown() throws InterruptedException {
        WritableEntrySource buffer = mock(WritableEntrySource.class);
        when(buffer.getInt(anyInt())).thenReturn(10);
        long segment = 10_000_000;
        Set<Long> completedSequences = new HashSet<>();
        Set<Long> cancelledSequences = new HashSet<>();
        synchronizer.init(new WritePosition(1, 1, buffer, segment));
        StorageCallback callback = new StorageCallback() {
            @Override
            public boolean onCompleted(long firstToken) {
                completedSequences.add(firstToken);
                return true;
            }

            @Override
            public void onError(Throwable cause) {
            }
        };
        synchronizer.register(new WritePosition(1, 1, buffer, segment), callback);
        synchronizer.notifyWritePositions();

        assertEquals(1, completedSequences.size());

        synchronizer.register(new WritePosition(2, 1, buffer, segment), callback);
        Executors.newSingleThreadScheduledExecutor().schedule(() -> synchronizer.notifyWritePositions(),
                                                              20,
                                                              TimeUnit.MILLISECONDS);
        synchronizer.shutdown(false);
        assertEquals(2, completedSequences.size());
        long newSegment = 10_000_000;
        synchronizer.init(new WritePosition(1, 1, buffer, newSegment));
        synchronizer.register(new WritePosition(3, 1, buffer, newSegment), callback);
        synchronizer.notifyWritePositions();
        assertEquals(3, completedSequences.size());
        Thread.sleep(100);
        assertEquals(0, completedSegments.size());
        synchronizer.register(new WritePosition(4, 1, buffer, newSegment + 1), callback);
        synchronizer.notifyWritePositions();
        Thread.sleep(100);
        assertEquals(1, completedSegments.size());
    }
}