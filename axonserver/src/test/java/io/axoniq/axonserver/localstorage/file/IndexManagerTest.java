package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 * Tests for {@link IndexManager}.
 *
 * @author Milan Savic
 */
public class IndexManagerTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private IndexManager indexManager;
    private StorageProperties storageProperties;
    private String context;
    private SystemInfoProvider systemInfoProvider;

    @Before
    public void setUp() throws IOException {
        context = "default";

        temporaryFolder.newFolder(context);
        systemInfoProvider = new SystemInfoProvider() {
        };
        storageProperties = new StorageProperties(systemInfoProvider);
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

    @Test
    public void testTemporaryFileIsDeletedWhenCreatingIndex() throws IOException {
        long segment = 0L;

        File tempFile = storageProperties.indexTemp(context, segment);
        try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            outputStream.write("mockDataToCreateIllegalFile".getBytes(StandardCharsets.UTF_8));
        }

        String aggregateId = "aggregateId";
        ConcurrentSkipListSet<PositionInfo> positions = new ConcurrentSkipListSet<>();
        PositionInfo positionInfo = new PositionInfo(0, 0);
        positions.add(positionInfo);
        Map<String, SortedSet<PositionInfo>> positionsPerAggregate = Collections.singletonMap(aggregateId,
                                                                                              positions);
        indexManager.createIndex(segment, positionsPerAggregate);

        assertFalse(storageProperties.indexTemp(context, segment).exists());
    }

    @Test(expected = MessagingPlatformException.class)
    public void testIndexCreationFailsIfTemporaryFileIsKeptOpen() throws IOException {
        assumeTrue(systemInfoProvider.javaOnWindows());
        long segment = 0L;

        File tempFile = storageProperties.indexTemp(context, segment);

        String aggregateId = "aggregateId";
        ConcurrentSkipListSet<PositionInfo> positions = new ConcurrentSkipListSet<>();
        PositionInfo positionInfo = new PositionInfo(0, 0);
        positions.add(positionInfo);
        Map<String, SortedSet<PositionInfo>> positionsPerAggregate = Collections.singletonMap(aggregateId,
                                                                                              positions);

        try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            outputStream.write("mockDataToCreateIllegalFile".getBytes(StandardCharsets.UTF_8));
            indexManager.createIndex(segment, positionsPerAggregate);
        }
    }
}
