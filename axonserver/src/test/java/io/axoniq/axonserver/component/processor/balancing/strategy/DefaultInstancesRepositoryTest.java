package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.strategy.ThreadNumberBalancing.Application;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import org.junit.*;

import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class DefaultInstancesRepositoryTest {

    @Test
    public void testRepositoryFiltersOutStoppedProcessorInstances() {
        ClientProcessor runningClientProcessor = new FakeClientProcessor("runningClientId",
                                                                         true,
                                                                         "processorName",
                                                                         true);
        ClientProcessor stoppedClientProcessor = new FakeClientProcessor("stoppedClientId",
                                                                         true,
                                                                         "processorName",
                                                                         false);
        ClientProcessors clientProcessors = () -> asList(runningClientProcessor, stoppedClientProcessor).iterator();
        DefaultInstancesRepository testSubject = new DefaultInstancesRepository(clientProcessors);

        TrackingEventProcessor eventProcessor = new TrackingEventProcessor("processorName");
        Iterator<Application> iterator = testSubject.findFor(eventProcessor).iterator();
        assertTrue(iterator.hasNext());
        assertTrue(iterator.next().toString().contains("runningClientId"));
        assertFalse(iterator.hasNext());
    }
}