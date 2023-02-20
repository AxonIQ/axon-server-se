/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.FileSystemMonitor;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.test.TestUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

/**
 * @author Marc Gathier
 */
public class RecreateIndexTest {

    private FileEventStorageEngine testSubject;
    private final FileSystemMonitor fileSystemMonitor = mock(FileSystemMonitor.class);

    @Before
    public void init() {

        File sampleEventStoreFolder = new File(TestUtils
                                                       .fixPathOnWindows(InputStreamStrorageTierEventStore.class
                                                                                 .getResource(
                                                                                         "/event-store-without-index")
                                                                                 .getFile()));

        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());

        StorageProperties storageProperties = new StorageProperties(new SystemInfoProvider() {
            @Override
            public String getHostName() throws UnknownHostException {
                return null;
            }
        });
        storageProperties.setStorage(sampleEventStoreFolder.getAbsolutePath());

        IndexManager indexManager = new StandardIndexManager("default",
                                                             () -> storageProperties,
                                                             storageProperties.getPrimaryStorage("default"),
                                                             EventType.EVENT,
                                                             meterFactory);

        InputStreamStrorageTierEventStore secondaryEventStore = new InputStreamStrorageTierEventStore(new EventTypeContext(
                "default",
                EventType.EVENT),
                                                                                                      indexManager,
                                                                                                      new DefaultEventTransformerFactory(),
                                                                                                      () -> storageProperties,
                                                                                                      meterFactory,
                                                                                                      storageProperties.getPrimaryStorage(
                                                                                                              "default"));

        doNothing().when(fileSystemMonitor).registerPath(any(), any());

        testSubject = new FileEventStorageEngine(new EventTypeContext("default", EventType.EVENT),
                                                 indexManager,
                                                 new DefaultEventTransformerFactory(),
                                                 () -> storageProperties,
                                                 () -> secondaryEventStore,
                                                 meterFactory, fileSystemMonitor,
                                                 storageProperties.getPrimaryStorage("default"));
    }

    @Test
    public void recreateIndex() {
        testSubject.init(true);

        List<String> files = testSubject.getBackupFilenames(-1, 0, false).collect(Collectors.toList());
        assertEquals(6, files.size());

        AtomicInteger events = new AtomicInteger();
        testSubject.processEventsPerAggregate("Aggregate-1", 0, Long.MAX_VALUE, 0,
                                              e -> events.incrementAndGet());

        assertEquals(14, events.get());
    }
}
