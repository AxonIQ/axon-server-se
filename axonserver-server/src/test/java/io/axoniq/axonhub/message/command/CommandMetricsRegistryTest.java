package io.axoniq.axonhub.message.command;

import com.codahale.metrics.MetricRegistry;
import io.axoniq.axonhub.cluster.ClusterMetricTarget;
import io.axoniq.axonhub.metric.HistogramFactory;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandMetricsRegistryTest {

    private CommandMetricsRegistry testSubject;

    private final MetricRegistry metricRegistry = new MetricRegistry();

    @Before
    public void setUp() {
        testSubject = new CommandMetricsRegistry(metricRegistry, new HistogramFactory(15), new ClusterMetricTarget());
    }

    @Test
    public void add() {
        testSubject.add("Command", "Client1",  1);
        assertEquals(1, metricRegistry.histogram(MetricRegistry.name("command", "Command", "Client1")).getCount());
    }

}