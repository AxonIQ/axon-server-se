package io.axoniq.axonserver.enterprise.message.query;

import io.axoniq.axonserver.enterprise.messaging.query.MetricsBasedQueryHandlerSelector;
import io.axoniq.axonserver.message.ClientIdentification;
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

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class MetricsBasedQueryHandlerSelectorTest {
    private MetricsBasedQueryHandlerSelector selector;
    private NavigableSet<ClientIdentification> handlers = new TreeSet<>();

    @Mock
    private QueryMetricsRegistry queryMetricsRegistry;
    private ClientIdentification client1;
    private ClientIdentification client2;

    @Before
    public void setUp()  {
        selector = new MetricsBasedQueryHandlerSelector(queryMetricsRegistry);
        client1 = new ClientIdentification(Topology.DEFAULT_CONTEXT, "client1");
        handlers.add(client1);
        client2 = new ClientIdentification(Topology.DEFAULT_CONTEXT, "client2");
        handlers.add(client2);
    }

    @Test
    public void selectWithNoMetrics()  {
        ClientIdentification selected = selector.select(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), "component1", handlers);
        Assert.assertEquals("client1", selected.getClient());
    }

    @Test
    public void selectBasedOnCount() {
        FakeClusterMetric clusterMetric1 = new FakeClusterMetric(15);
        FakeClusterMetric clusterMetric2 = new FakeClusterMetric(5);
        Mockito.when(queryMetricsRegistry.clusterMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), client1.toString())).thenReturn(clusterMetric1);
        Mockito.when(queryMetricsRegistry.clusterMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), client2.toString())).thenReturn(clusterMetric2);
        ClientIdentification selected = selector.select(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), "component1", handlers);
        Assert.assertEquals("client2", selected.getClient());
    }
    @Test
    public void selectBasedOnMean() {
        FakeClusterMetric clusterMetric1 = new FakeClusterMetric(1500, 0.1);
        FakeClusterMetric clusterMetric2 = new FakeClusterMetric(500, 0.2);
        Mockito.when(queryMetricsRegistry.clusterMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), client1.toString())).thenReturn(clusterMetric1);
        Mockito.when(queryMetricsRegistry.clusterMetric(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), client2.toString())).thenReturn(clusterMetric2);

        ClientIdentification selected = selector.select(new QueryDefinition(Topology.DEFAULT_CONTEXT, "request"), "component1", handlers);
        Assert.assertEquals("client1", selected.getClient());
    }

}