/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import static io.axoniq.axonserver.topology.Topology.DEFAULT_CONTEXT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marc Gathier
 */
public class QueryMetricsRegistryTest {

    private QueryMetricsRegistry testSubject;
    private ClientStreamIdentification clientIdentification = new ClientStreamIdentification(DEFAULT_CONTEXT,
                                                                                             "processor");
    private SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    @Before
    public void setUp() {
        testSubject = new QueryMetricsRegistry(new MeterFactory(meterRegistry,
                                                                new DefaultMetricCollector()));
    }

    @Test
    public void add() {
        QueryDefinition queryDef = new QueryDefinition(DEFAULT_CONTEXT, "a");
        testSubject.addHandlerResponseTime(queryDef,
                                           "source",
                                           "target",
                                           DEFAULT_CONTEXT,
                                           1L);
        Search search = meterRegistry.find(BaseMetricName.AXON_QUERY.metric());
        assertTrue(search.timers().stream().anyMatch(t -> "source".equals(t.getId().getTag(MeterFactory.SOURCE))));
    }

    @Test
    public void addAfterDisconnect() {
        QueryDefinition queryDef = new QueryDefinition(DEFAULT_CONTEXT, "a");
        testSubject.addHandlerResponseTime(queryDef,
                                           "source",
                                           "target",
                                           DEFAULT_CONTEXT,
                                           1L);
        testSubject.on( new TopologyEvents.ApplicationDisconnected(DEFAULT_CONTEXT, "component", "source", "disconnected"));
        Search search = meterRegistry.find(BaseMetricName.AXON_QUERY.metric());
        assertTrue(search.timers().stream().noneMatch(t -> "source".equals(t.getId().getTag(MeterFactory.SOURCE))));
        assertTrue(search.timers().stream().noneMatch(t -> "source".equals(t.getId().getTag(MeterFactory.TARGET))));
        testSubject.addHandlerResponseTime(queryDef,
                                           "source",
                                           "target",
                                           DEFAULT_CONTEXT,
                                           1L);
        search = meterRegistry.find(BaseMetricName.AXON_QUERY.metric());
        assertTrue(search.timers().stream().anyMatch(t -> "source".equals(t.getId().getTag(MeterFactory.SOURCE))));
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
