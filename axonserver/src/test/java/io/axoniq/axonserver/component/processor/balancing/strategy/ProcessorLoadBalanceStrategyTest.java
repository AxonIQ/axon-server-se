package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.ApplicationProcessorEventsSource;
import io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Author: marc
 */
public class ProcessorLoadBalanceStrategyTest {

    private ProcessorLoadBalanceStrategy testSubject;

    @Before
    public void setUp()  {
        Map<String, LoadBalancingStrategy.Factory> factories =
                new HashMap<>();
        factories.put("NoLoadBalance", new NoLoadBalanceStrategy.Factory());
        ApplicationProcessorEventsSource processorEventsSource = mock(ApplicationProcessorEventsSource.class);
        ClientProcessors processors = Collections::emptyListIterator;
        factories.put("ThreadNumberBalancingStrategy",
                      new ThreadNumberBalancing.ThreadNumberBalancingStrategyFactory(processorEventsSource, processors));
        testSubject = new ProcessorLoadBalanceStrategy(new SimpleLoadBalanceStrategyHolder(), factories);
    }

    @Test
    public void balanceThreadNumber() {
        testSubject.balance(new TrackingEventProcessor("testProcessor", "testComponent", "default"), "threadNumber").perform();
    }

    @Test
    public void balanceDefault() {
        testSubject.balance(new TrackingEventProcessor("testProcessor", "testComponent", "default"), "default").perform();
    }

}