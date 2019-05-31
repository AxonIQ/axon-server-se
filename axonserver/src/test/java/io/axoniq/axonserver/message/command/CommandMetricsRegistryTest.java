/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandMetricsRegistryTest {

    private CommandMetricsRegistry testSubject;

    @Before
    public void setUp() {
        testSubject = new CommandMetricsRegistry(new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector()));
    }

    @Test
    public void add() {
        ClientIdentification client1 = new ClientIdentification(Topology.DEFAULT_CONTEXT, "Client1");
        testSubject.add("Command", client1, 1);

        assertEquals(1L, testSubject.commandMetric("Command", client1, null).getCount());
    }

}
