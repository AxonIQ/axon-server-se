package io.axoniq.axonhub.component.processor;

import io.axoniq.axonhub.component.processor.listener.ClientProcessor;
import io.axoniq.axonhub.component.processor.listener.FakeClientProcessor;
import io.axoniq.platform.grpc.EventProcessorInfo;
import org.junit.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 27/03/2018.
 * sara.pellegrini@gmail.com
 */
public class EventProcessorMappingTest {

    private final EventProcessorMapping processorMapping = new EventProcessorMapping();

    @Test
    public void testTracking(){
        ClientProcessor clientProcessor = new FakeClientProcessor("clientId", true,
                                                                  EventProcessorInfo.newBuilder().setMode("Tracking").build());

        EventProcessor processor = processorMapping.apply("processorName",
                                                              asList(clientProcessor, clientProcessor));

        assertTrue(processor instanceof TrackingProcessor);
    }

    @Test
    public void testGeneric(){
        ClientProcessor clientProcessor = new FakeClientProcessor("clientId", true,
                                                                  EventProcessorInfo.newBuilder().setMode("Subscribing").build());

        EventProcessor processor = processorMapping.apply("processorName",
                                                          asList(clientProcessor, clientProcessor));

        assertTrue(processor instanceof GenericProcessor);
        assertEquals("Subscribing", processor.mode());
    }

    @Test
    public void testMixed(){
        ClientProcessor tracking = new FakeClientProcessor("clientId", true,
                                                           EventProcessorInfo.newBuilder().setMode("Tracking").build());

        ClientProcessor subscribing = new FakeClientProcessor("clientId", true,
                                                              EventProcessorInfo.newBuilder().setMode("Subscribing").build());

        EventProcessor processor = processorMapping.apply("processorName",  asList(tracking, subscribing));

        assertTrue(processor instanceof GenericProcessor);
        assertNotEquals("Tracking", processor.mode());
        assertNotEquals("Subscribing", processor.mode());
    }

}