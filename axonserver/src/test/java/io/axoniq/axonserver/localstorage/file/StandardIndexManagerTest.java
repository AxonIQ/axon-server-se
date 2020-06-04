package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.rules.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 * Tests for {@link StandardIndexManager}.
 *
 * @author Milan Savic
 */
public class StandardIndexManagerTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private StandardIndexManager indexManager;
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

        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());
        indexManager = new StandardIndexManager(context, storageProperties, meterFactory);
    }

    @Test
    public void testConcurrentAccess() {
        long segment = 0L;
        String aggregateId = "aggregateId";
        IndexEntry positionInfo = new IndexEntry(0, 0, 0);
        indexManager.addToActiveSegment(segment, "aa", positionInfo);
        indexManager.complete(segment);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        int concurrentRequests = 100;
        Future[] futures = new Future[concurrentRequests];
        for (int i = 0; i < concurrentRequests; i++) {
            Future<?> future = executorService.submit(() -> {
                IndexEntries actual = indexManager.positions(segment, aggregateId);
                assertEquals(positionInfo.getSequenceNumber(), actual.firstSequenceNumber());
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
        IndexEntry positionInfo = new IndexEntry(0, 0, 0);
        indexManager.addToActiveSegment(segment, aggregateId, positionInfo);
        indexManager.complete(segment);

        assertFalse(storageProperties.indexTemp(context, segment).exists());
    }

    @Test(expected = MessagingPlatformException.class)
    public void testIndexCreationFailsIfTemporaryFileIsKeptOpen() throws IOException {
        assumeTrue(systemInfoProvider.javaOnWindows());
        long segment = 0L;

        File tempFile = storageProperties.indexTemp(context, segment);

        String aggregateId = "aggregateId";
        IndexEntry positionInfo = new IndexEntry(0, 0, 0);

        try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            outputStream.write("mockDataToCreateIllegalFile".getBytes(StandardCharsets.UTF_8));
            indexManager.addToActiveSegment(segment, aggregateId, positionInfo);
            indexManager.complete(segment);
        }
    }
}
