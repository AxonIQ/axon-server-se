package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;

/**
 * @author Sara Pellegrini
 */
@RunWith(MockitoJUnitRunner.class)
public class LocalEventProcessorsAdminServiceTest {

    @Mock
    ProcessorEventPublisher publisher;

    @Test
    public void pauseTest() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors);
        testSubject.pause(new EventProcessorIdentifier(processorName, tokenStore), () -> "authenticated-user")
                   .block();
        verify(publisher).pauseProcessorRequest(eq("default"), eq("Client-A"), eq(processorName));
        verify(publisher).pauseProcessorRequest(eq("default"), eq("Client-B"), eq(processorName));
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void startTest() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors);
        testSubject.start(new EventProcessorIdentifier(processorName, tokenStore), () -> "authenticated-user")
                   .block();
        verify(publisher).startProcessorRequest(eq("default"), eq("Client-A"), eq(processorName));
        verify(publisher).startProcessorRequest(eq("default"), eq("Client-B"), eq(processorName));
        verifyNoMoreInteractions(publisher);
    }
}