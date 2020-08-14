/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class QueryMetricsRegistryTest {

    private QueryMetricsRegistry testSubject;
    private ClientStreamIdentification clientIdentification = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                             "processor");

    @Before
    public void setUp() {
        testSubject = new QueryMetricsRegistry(new MeterFactory(new SimpleMeterRegistry(),
                                                                new DefaultMetricCollector()));
    }

    @Test
    public void add() {
        testSubject.add(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), "source", clientIdentification, 1L);
    }

    @Test
    public void get()  {
        testSubject.add(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), "source", clientIdentification, 1L);
        QueryMetricsRegistry.QueryMetric queryMetric = testSubject
                .queryMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), clientIdentification, "");
        assertEquals(1, queryMetric.getCount());
        queryMetric = testSubject.queryMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"),
                                              new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                             "processor1"),
                                              "");
        assertEquals(0, queryMetric.getCount());
    }

}
