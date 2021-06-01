package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.topology.Topology;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

public class DefaultInstancesRepositoryTest {

    @Test
    public void testRepositoryFiltersOutStoppedProcessorInstances() {
        DefaultInstancesRepository testSubject = new DefaultInstancesRepository(() -> Arrays.<ClientProcessor>asList(new FakeClientProcessor("runningClientId", true, "processorName", true),
                                                                                                                     new FakeClientProcessor("stoppedClientId", true, "processorName", false)).iterator());

        Iterator<ThreadNumberBalancing.Application> actual = testSubject.findFor(new TrackingEventProcessor("processorName", Topology.DEFAULT_CONTEXT)).iterator();
        assertTrue(actual.hasNext());
        assertTrue(actual.next().toString().contains("runningClientId"));
        assertFalse(actual.hasNext());
    }
}