/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.test.TestUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

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

        indexManager = new StandardIndexManager(context,
                                                () -> storageProperties,
                                                storageProperties.getPrimaryStorage(context),
                                                EventType.EVENT,
                                                meterFactory,
                                                () -> null);
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
                SortedMap<FileVersion, IndexEntries> actual = indexManager.lookupAggregate(aggregateId,
                                                                                    0,
                                                                                    Long.MAX_VALUE,
                                                                                    Long.MAX_VALUE, 0);
                assertEquals(positionInfo.getSequenceNumber(), actual.get(new FileVersion(0L, 0)).firstSequenceNumber());
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

        SortedMap<FileVersion, IndexEntries> position = indexManager.lookupAggregate(aggregateId, 0, 5, 100, 0);
        assertEquals(1, position.size());
        assertNotNull(position.get(new FileVersion(0L, 0)));
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

        SortedMap<FileVersion, IndexEntries> position = indexManager.lookupAggregate(aggregateId, 0, Long.MAX_VALUE, 100, 11);
        assertEquals(2, position.size());
        assertNotNull(position.get(new FileVersion(10L, 0)));
        assertNotNull(position.get(new FileVersion(15, 0)));
        position = indexManager.lookupAggregate(aggregateId, 0, Long.MAX_VALUE, 100, 15);
        assertEquals(2, position.size());
        assertNotNull(position.get(new FileVersion(15L, 0)));
    }

    @Test
    public void testTemporaryFileIsDeletedWhenCreatingIndex() throws IOException {
        long segment = 0L;

        File tempFile = storageProperties.transformedIndex(storageProperties.getPrimaryStorage(context), segment);
        try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            outputStream.write("mockDataToCreateIllegalFile".getBytes(StandardCharsets.UTF_8));
        }

        String aggregateId = "aggregateId";
        IndexEntry positionInfo = new IndexEntry(0, 0, 0);
        indexManager.addToActiveSegment(segment, aggregateId, positionInfo);
        indexManager.complete(segment);

        assertFalse(storageProperties.transformedIndex(storageProperties.getPrimaryStorage(context), segment).exists());
    }

    @Test(expected = MessagingPlatformException.class)
    public void testIndexCreationFailsIfTemporaryFileIsKeptOpen() throws IOException {
        assumeTrue(systemInfoProvider.javaOnWindows());
        long segment = 0L;

        File tempFile = storageProperties.transformedIndex(storageProperties.getPrimaryStorage(context), segment);

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
                                                               .getResource(
                                                                       "/event-store-without-domain-events-in-last-segment")
                                                               .getFile());
        storageProperties = new StorageProperties(systemInfoProvider);
        storageProperties.setMaxIndexesInMemory(3);
        storageProperties.setStorage(eventStore);
        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());

        indexManager = new StandardIndexManager(context,
                                                () -> storageProperties,
                                                storageProperties.getPrimaryStorage(context),
                                                EventType.EVENT,
                                                meterFactory,
                                                () -> null);
        indexManager.init();

        Optional<Long> result = indexManager.getLastSequenceNumber("Aggregate-25", Integer.MAX_VALUE, Long.MAX_VALUE);

        Assertions.assertThat(result).isNotEmpty();
    }

}
