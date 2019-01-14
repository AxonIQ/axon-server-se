package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandMetricsRegistryTest {

    private CommandMetricsRegistry testSubject;

    @Before
    public void setUp() {
        testSubject = new CommandMetricsRegistry(new SimpleMeterRegistry(), new DefaultMetricCollector());
    }

    @Test
    public void add() {
        testSubject.add("Command", "Client1",  1);

        assertEquals(1L, testSubject.commandMetric("Command", "Client1", null).getCount());
    }

}
