package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.enterprise.cluster.ClusterMetricTarget;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry.QueryMetric;
import io.micrometer.core.instrument.Metrics;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class QueryMetricsRegistryTest {
    private QueryMetricsRegistry testSubject;

    @Before
    public void setUp() {
        testSubject = new QueryMetricsRegistry(Metrics.globalRegistry, new ClusterMetricTarget());
    }

    @Test
    public void add() {
        testSubject.add(new QueryDefinition(ContextController.DEFAULT, "a"), "processor", 1L);
    }

    @Test
    public void get()  {
        testSubject.add(new QueryDefinition(ContextController.DEFAULT, "a"), "processor", 1L);
        QueryMetric queryMetric = testSubject.queryMetric(new QueryDefinition(ContextController.DEFAULT, "a"), "processor", "");
        assertEquals(1, queryMetric.getCount());
        queryMetric = testSubject.queryMetric(new QueryDefinition(ContextController.DEFAULT, "a"), "processor1", "");
        assertEquals(0, queryMetric.getCount());
    }

}