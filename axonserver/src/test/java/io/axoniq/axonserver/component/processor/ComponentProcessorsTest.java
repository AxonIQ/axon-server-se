package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.*;

import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ComponentProcessorsTest {

    @Test
    public void testOne() {
        ClientProcessor clientProcessor = new FakeClientProcessor("clientId", true, EventProcessorInfo.getDefaultInstance());

        ClientProcessors clientProcessors = () -> asList(new FakeClientProcessor("clientId", false, EventProcessorInfo.getDefaultInstance()),
                                                         clientProcessor,
                                                         new FakeClientProcessor("clientId", false, EventProcessorInfo.getDefaultInstance()))
                .iterator();

        ComponentProcessors processors = new ComponentProcessors("component", Topology.DEFAULT_CONTEXT, clientProcessors);
        Iterator<EventProcessor> iterator = processors.iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }




}