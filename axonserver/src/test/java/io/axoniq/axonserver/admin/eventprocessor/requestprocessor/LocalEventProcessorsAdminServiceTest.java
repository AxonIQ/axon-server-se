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

import java.util.List;

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
        testSubject.pause(new EventProcessorIdentifier(processorName, tokenStore), () -> "authenticated-user");
        verify(publisher).pauseProcessorRequest("default", "Client-A", processorName);
        verify(publisher).pauseProcessorRequest("default", "Client-B", processorName);
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
        testSubject.start(new EventProcessorIdentifier(processorName, tokenStore), () -> "authenticated-user");
        verify(publisher).startProcessorRequest("default", "Client-A", processorName);
        verify(publisher).startProcessorRequest("default", "Client-B", processorName);
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void splitTest() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors);
        testSubject.split(new EventProcessorIdentifier(processorName, tokenStore), () -> "authenticated-user");
        List<String> clients = asList("Client-A", "Client-B");
        verify(publisher).splitSegment("default", clients, processorName);
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void mergeTest() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors);
        testSubject.merge(new EventProcessorIdentifier(processorName, tokenStore), () -> "authenticated-user");
        List<String> clients = asList("Client-A", "Client-B");
        verify(publisher).mergeSegment("default", clients, processorName);
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void testMoveSegment() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessor clientE = new FakeClientProcessor("Client-E", processorName, tokenStore);
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD, clientE).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors);
        testSubject.move(new EventProcessorIdentifier(processorName, tokenStore), 2, "Client-B",
                         () -> "authenticated-user");
        verify(publisher).releaseSegment("default", "Client-A", processorName, 2);
        verify(publisher).releaseSegment("default", "Client-E", processorName, 2);
        verifyNoMoreInteractions(publisher);
    }
}