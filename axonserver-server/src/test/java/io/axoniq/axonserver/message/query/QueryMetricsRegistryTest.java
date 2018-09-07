package io.axoniq.axonserver.message.query;

import com.codahale.metrics.MetricRegistry;
import io.axoniq.axonserver.cluster.ClusterMetricTarget;
import io.axoniq.axonserver.context.ContextController;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry.QueryMetric;
import io.axoniq.axonserver.metric.HistogramFactory;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class QueryMetricsRegistryTest {
    private QueryMetricsRegistry testSubject;
    private final MetricRegistry metricRegistry = new MetricRegistry();

    @Before
    public void setUp() {
        testSubject = new QueryMetricsRegistry(metricRegistry, new HistogramFactory(15),
                                               new ClusterMetricTarget());
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