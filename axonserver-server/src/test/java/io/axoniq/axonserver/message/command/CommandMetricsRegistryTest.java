package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.cluster.ClusterMetricTarget;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandMetricsRegistryTest {

    private CommandMetricsRegistry testSubject;

    @Before
    public void setUp() {
        testSubject = new CommandMetricsRegistry(new SimpleMeterRegistry(), new ClusterMetricTarget());
    }

    @Test
    public void add() {
        testSubject.add("Command", "Client1",  1);

        assertEquals(1L, testSubject.commandMetric("Command", "Client1", null).getCount());
    }

}