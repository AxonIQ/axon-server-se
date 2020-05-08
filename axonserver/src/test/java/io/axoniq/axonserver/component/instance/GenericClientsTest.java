/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.instance;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;

import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class GenericClientsTest {

    private GenericClients testSubject = new GenericClients(new MessagingPlatformConfiguration(new SystemInfoProvider() {
        @Override
        public String getHostName() {
            return "localhost";
        }
    }));


    @Test
    public void concurrentModification() {
        testSubject.on(new TopologyEvents.ApplicationConnected(Topology.DEFAULT_CONTEXT, "application", "1@node"));
        Iterator<Client> iterator = testSubject.iterator();
        if (iterator.hasNext()) {
            testSubject.on(new TopologyEvents.ApplicationConnected(Topology.DEFAULT_CONTEXT, "application", "1@node2"));
            Client next = iterator.next();
            assertNotNull(next);
        }
    }
}
