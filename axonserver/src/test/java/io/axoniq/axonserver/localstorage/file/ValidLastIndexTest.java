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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

/**
 * @author Marc Gathier
 */
public class ValidLastIndexTest {

    private FileEventStorageEngine testSubject;
    private final FileSystemMonitor fileSystemMonitor = mock(FileSystemMonitor.class);


    File sampleEventStoreFolder = new File(TestUtils
                                                   .fixPathOnWindows(InputStreamStrorageTierEventStore.class
                                                                             .getResource(
                                                                                     "/event-store-with-last-segment-index")
                                                                             .getFile()));

    @Before
    public void init() {
        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());
        doNothing().when(fileSystemMonitor).registerPath(any(), any());

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
        testSubject = new FileEventStorageEngine(new EventTypeContext("default", EventType.EVENT),
                                                 indexManager,
                                                 new DefaultEventTransformerFactory(),
                                                 () -> storageProperties,
                                                 () -> secondaryEventStore,
                                                 meterFactory, fileSystemMonitor,
                                                 storageProperties.getPrimaryStorage("default"));
    }

    @Test
    public void startWithNewSegment() {
        testSubject.init(true);
        assertTrue(new File(sampleEventStoreFolder.getAbsolutePath() + "/default/00000000000000000462.events")
                           .exists());
    }
}
