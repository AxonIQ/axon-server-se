/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Marc Gathier
 */
public class QueryMetricsRegistryTest {
    private static final ClientIdentification CLIENT_IDENTIFICATION = new ClientIdentification(Topology.DEFAULT_CONTEXT, "processor");

    private QueryMetricsRegistry testSubject;

    @Before
    public void setUp() {
        testSubject = new QueryMetricsRegistry(new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector()));
    }

    @Test
    public void add() {
        testSubject.add(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), "source", CLIENT_IDENTIFICATION, 1L);
    }

    @Test
    public void getSome() {
        testSubject.add(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), "source", CLIENT_IDENTIFICATION, 1L);
        testSubject.add(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), "source", CLIENT_IDENTIFICATION, 2L);

        QueryMetricsRegistry.QueryMetric queryMetric = testSubject
                .queryMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), CLIENT_IDENTIFICATION, "");

        assertEquals(2L, queryMetric.getCount());
        assertEquals(1.5, queryMetric.getMean(), Double.MIN_VALUE);
        assertEquals(2.0, queryMetric.getMax(), Double.MIN_VALUE);
    }

    @Test
    public void getNone() {
        QueryMetricsRegistry.QueryMetric queryMetric = testSubject.queryMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"),
                new ClientIdentification(Topology.DEFAULT_CONTEXT, "processor1"),
                "");
        assertEquals(0, queryMetric.getCount());
    }

    @Test
    public void testErrorRates() {
        MeterFactory.RateMeter errorRate1 = testSubject.errorRate("testContext", BaseMetricName.AXON_QUERY_RATE, "testError1");
        MeterFactory.RateMeter errorRate2 = testSubject.errorRate("testContext", BaseMetricName.AXON_QUERY_RATE, "testError2");

        errorRate1.mark();
        errorRate1.mark();
        errorRate2.mark();

        assertEquals(2L, errorRate1.getCount());
        assertEquals(1L, errorRate2.getCount());
    }

}
