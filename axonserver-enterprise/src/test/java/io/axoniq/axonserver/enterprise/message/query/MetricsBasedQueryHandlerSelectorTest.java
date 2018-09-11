package io.axoniq.axonserver.enterprise.message.query;

import io.axoniq.axonserver.enterprise.messaging.query.MetricsBasedQueryHandlerSelector;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.metric.FakeClusterMetric;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class MetricsBasedQueryHandlerSelectorTest {
    private MetricsBasedQueryHandlerSelector selector;
    private NavigableSet<String> handlers = new TreeSet<>();

    @Mock
    private QueryMetricsRegistry queryMetricsRegistry;

    @Before
    public void setUp()  {
        selector = new MetricsBasedQueryHandlerSelector(queryMetricsRegistry);
        handlers.add( "client1");
        handlers.add( "client2");
    }

    @Test
    public void selectWithNoMetrics()  {
        String selected = selector.select(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), "component1", handlers);
        Assert.assertEquals("client1", selected);
    }

    @Test
    public void selectBasedOnCount() {
        FakeClusterMetric clusterMetric1 = new FakeClusterMetric(15);
        FakeClusterMetric clusterMetric2 = new FakeClusterMetric(5);
        Mockito.when(queryMetricsRegistry.clusterMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), "client1")).thenReturn(clusterMetric1);
        Mockito.when(queryMetricsRegistry.clusterMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), "client2")).thenReturn(clusterMetric2);
        String selected = selector.select(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), "component1", handlers);
        Assert.assertEquals("client2", selected);
    }
    @Test
    public void selectBasedOnMean() {
        FakeClusterMetric clusterMetric1 = new FakeClusterMetric(1500, 0.1);
        FakeClusterMetric clusterMetric2 = new FakeClusterMetric(500, 0.2);
        Mockito.when(queryMetricsRegistry.clusterMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), "client1")).thenReturn(clusterMetric1);
        Mockito.when(queryMetricsRegistry.clusterMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), "client2")).thenReturn(clusterMetric2);

        String selected = selector.select(new QueryDefinition(Topology.DEFAULT_CONTEXT,"request"), "component1", handlers);
        Assert.assertEquals("client1", selected);
    }

}