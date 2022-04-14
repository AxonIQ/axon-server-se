/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.grpc.Gateway;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.plugin.OsgiController;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.unit.DataSize;

import java.time.Duration;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest(properties = {
        "axoniq.axonserver.max-message-size=10MB",
        "axoniq.axonserver.accesscontrol.cache-ttl=15M",
        "axoniq.axonserver.event.segment-size=143MB",
        "axon.axonserver.enabled=false"}
)
public class ConfigurationPropertiesTest {

    @Autowired
    private MessagingPlatformConfiguration messagingPlatformConfiguration;
    @Autowired
    private EmbeddedDBProperties embeddedDBProperties;

    @MockBean
    private LocalEventStore localEventStore;

    @MockBean
    private OsgiController osgiController;

    @MockBean
    private Gateway gateway;

    @Test
    public void testMaxMessageSize() {
        assertEquals(DataSize.ofMegabytes(10).toBytes(), messagingPlatformConfiguration.getMaxMessageSize());
    }

    @Test
    public void testCacheTtl() {
        assertEquals(Duration.ofMinutes(15).toMillis(),
                     messagingPlatformConfiguration.getAccesscontrol().getCacheTtl());
    }

    @Test
    public void testSegmentSize() {
        assertEquals(DataSize.ofMegabytes(143).toBytes(), embeddedDBProperties.getEvent().getSegmentSize());
    }
}