package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import org.junit.*;
import org.junit.rules.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

/**
 * Tests for {@link IndexManager}.
 *
 * @author Milan Savic
 */
public class IndexManagerTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private IndexManager indexManager;

    @Before
    public void setUp() throws IOException {
        String context = "default";

        temporaryFolder.newFolder(context);

        StorageProperties storageProperties = new StorageProperties(new SystemInfoProvider() { });
        storageProperties.setMaxIndexesInMemory(3);
        storageProperties.setStorage(temporaryFolder.getRoot().getAbsolutePath());

        indexManager = new IndexManager(context, storageProperties);
    }

    @Test
    public void testConcurrentAccess() {
        long segment = 0L;
        String aggregateId = "aggregateId";
        ConcurrentSkipListSet<PositionInfo> positions = new ConcurrentSkipListSet<>();
        PositionInfo positionInfo = new PositionInfo(0, 0);
        positions.add(positionInfo);
        Map<String, SortedSet<PositionInfo>> positionsPerAggregate = Collections.singletonMap(aggregateId,
                                                                                              positions);
        indexManager.createIndex(segment, positionsPerAggregate);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        int concurrentRequests = 100;
        Future[] futures = new Future[concurrentRequests];
        for (int i = 0; i < concurrentRequests; i++) {
            Future<?> future = executorService.submit(() -> {
                SortedSet<PositionInfo> actual = indexManager.getPositions(segment, aggregateId);
                assertEquals(positionInfo, actual.first());
            });
            futures[i] = future;
        }

        // we should complete all getters successfully
        Arrays.stream(futures).forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                fail("Futures should complete successfully.");
            }
        });
    }
}