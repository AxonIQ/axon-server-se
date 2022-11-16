package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.test.TestUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.*;
import org.junit.rules.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.SortedMap;
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
        indexManager = new StandardIndexManager(context, () -> storageProperties, EventType.EVENT, meterFactory);
    }

    @Test
    public void testConcurrentAccess() {
        long segment = 0L;
        String aggregateId = "aggregateId";
        IndexEntry positionInfo = new IndexEntry(0, 0, 0);
        indexManager.addToActiveSegment(segment, aggregateId, positionInfo);
        indexManager.complete(segment);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        int concurrentRequests = 100;
        Future[] futures = new Future[concurrentRequests];
        for (int i = 0; i < concurrentRequests; i++) {
            Future<?> future = executorService.submit(() -> {
                SortedMap<Long, IndexEntries> actual = indexManager.lookupAggregate(aggregateId,
                                                                                    0,
                                                                                    Long.MAX_VALUE,
                                                                                    Long.MAX_VALUE, 0);
                assertEquals(positionInfo.getSequenceNumber(), actual.get(0L).firstSequenceNumber());
            });
            futures[i] = future;
        }

        // we should complete all getters successfully
        Arrays.stream(futures).forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                fail("Futures should complete successfully.");
            }
        });
    }

    @Test
    public void testIndexRange() {
        long segment = 0L;
        String aggregateId = "aggregateId";
        IndexEntry positionInfo = new IndexEntry(0, 0, 0);
        indexManager.addToActiveSegment(segment, aggregateId, positionInfo);
        indexManager.addToActiveSegment(segment, aggregateId, new IndexEntry(1, 0, 0));
        indexManager.addToActiveSegment(segment, aggregateId, new IndexEntry(2, 0, 0));
        indexManager.addToActiveSegment(segment, aggregateId, new IndexEntry(3, 0, 0));
        indexManager.addToActiveSegment(segment, aggregateId, new IndexEntry(4, 0, 0));
        indexManager.complete(0);
        indexManager.addToActiveSegment(10L, aggregateId, new IndexEntry(5, 0, 0));
        indexManager.addToActiveSegment(10L, aggregateId, new IndexEntry(6, 0, 0));
        indexManager.complete(10);
        indexManager.addToActiveSegment(15L, aggregateId, new IndexEntry(7, 0, 0));

        SortedMap<Long, IndexEntries> position = indexManager.lookupAggregate(aggregateId, 0, 5, 100, 0);
        assertEquals(1, position.size());
        assertNotNull(position.get(0L));
    }

    @Test
    public void testIndexMinToken() {
        long segment = 0L;
        String aggregateId = "aggregateId";
        IndexEntry positionInfo = new IndexEntry(0, 0, 0);
        indexManager.addToActiveSegment(segment, aggregateId, positionInfo);
        indexManager.addToActiveSegment(segment, aggregateId, new IndexEntry(1, 0, 0));
        indexManager.addToActiveSegment(segment, aggregateId, new IndexEntry(2, 0, 0));
        indexManager.addToActiveSegment(segment, aggregateId, new IndexEntry(3, 0, 0));
        indexManager.addToActiveSegment(segment, aggregateId, new IndexEntry(4, 0, 0));
        indexManager.complete(0);
        indexManager.addToActiveSegment(10L, aggregateId, new IndexEntry(5, 0, 0));
        indexManager.addToActiveSegment(10L, aggregateId, new IndexEntry(6, 0, 0));
        indexManager.complete(10);
        indexManager.addToActiveSegment(15L, aggregateId, new IndexEntry(7, 0, 0));

        SortedMap<Long, IndexEntries> position = indexManager.lookupAggregate(aggregateId, 0, Long.MAX_VALUE, 100, 11);
        assertEquals(2, position.size());
        assertNotNull(position.get(10L));
        assertNotNull(position.get(15L));
        position = indexManager.lookupAggregate(aggregateId, 0, Long.MAX_VALUE, 100, 15);
        assertEquals(2, position.size());
        assertNotNull(position.get(15L));
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

    @Test
    public void testLastSequenceNumberWhenNoDomainEventsInActiveIndexes() {
        String eventStore = TestUtils.fixPathOnWindows(StandardIndexManagerTest.class
                                                          .getResource("/event-store-without-domain-events-in-last-segment")
                                                          .getFile());
        storageProperties.setStorage(eventStore);
        indexManager.init();

        Optional<Long> result = indexManager.getLastSequenceNumber("Aggregate-25", Integer.MAX_VALUE, Long.MAX_VALUE);

        Assertions.assertThat(result).isNotEmpty();
    }

}
