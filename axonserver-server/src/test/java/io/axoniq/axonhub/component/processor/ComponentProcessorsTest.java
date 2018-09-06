package io.axoniq.axonhub.component.processor;

import io.axoniq.axonhub.component.processor.listener.ClientProcessor;
import io.axoniq.axonhub.component.processor.listener.ClientProcessors;
import io.axoniq.axonhub.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonhub.context.ContextController;
import io.axoniq.platform.grpc.EventProcessorInfo;
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

        ComponentProcessors processors = new ComponentProcessors("component", ContextController.DEFAULT, clientProcessors);
        Iterator<EventProcessor> iterator = processors.iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }




}