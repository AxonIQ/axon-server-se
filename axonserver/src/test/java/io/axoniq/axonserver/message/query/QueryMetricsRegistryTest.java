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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import static io.axoniq.axonserver.topology.Topology.DEFAULT_CONTEXT;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class QueryMetricsRegistryTest {

    private QueryMetricsRegistry testSubject;
    private ClientStreamIdentification clientIdentification = new ClientStreamIdentification(DEFAULT_CONTEXT,
                                                                                             "processor");

    @Before
    public void setUp() {
        testSubject = new QueryMetricsRegistry(new MeterFactory(new SimpleMeterRegistry(),
                                                                new DefaultMetricCollector()));
    }

    @Test
    public void add() {
        testSubject.addHandlerResponseTime(new QueryDefinition(DEFAULT_CONTEXT, "a"),
                                           "source",
                                           "target",
                                           DEFAULT_CONTEXT,
                                           1L);
    }

    @Test
    public void get() {
        QueryDefinition queryDefinition = new QueryDefinition(DEFAULT_CONTEXT, "a");
        testSubject.addHandlerResponseTime(queryDefinition, "source", "target", DEFAULT_CONTEXT, 1L);
        QueryMetricsRegistry.QueryMetric queryMetric = testSubject
                .queryMetric(queryDefinition, "target", DEFAULT_CONTEXT, "");
        assertEquals(1, queryMetric.getCount());
        queryMetric = testSubject.queryMetric(queryDefinition, "target1",
                                              DEFAULT_CONTEXT,
                                              "");
        assertEquals(0, queryMetric.getCount());
    }

}
