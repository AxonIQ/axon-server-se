package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry.QueryMetric;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class QueryMetricsRegistryTest {
    private QueryMetricsRegistry testSubject;
    private ClientIdentification clientIdentification = new ClientIdentification(Topology.DEFAULT_CONTEXT, "processor");
    @Before
    public void setUp() {
        testSubject = new QueryMetricsRegistry(new SimpleMeterRegistry(), new DefaultMetricCollector());
    }

    @Test
    public void add() {
        testSubject.add(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), clientIdentification, 1L);
    }

    @Test
    public void get()  {
        testSubject.add(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), clientIdentification, 1L);
        QueryMetric queryMetric = testSubject.queryMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), clientIdentification, "");
        assertEquals(1, queryMetric.getCount());
        queryMetric = testSubject.queryMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "a"), new ClientIdentification(Topology.DEFAULT_CONTEXT, "processor1"), "");
        assertEquals(0, queryMetric.getCount());
    }

}