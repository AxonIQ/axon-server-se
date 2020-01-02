package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.enterprise.cluster.events.serializer.EventProcessorStatusUpdateSerializer;
import io.axoniq.axonserver.enterprise.cluster.internal.MessagingClusterService;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for {@link RemoteEventTarget}
 *
 * @author Sara Pellegrini
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {RemoteEventTarget.class, EventProcessorStatusUpdateSerializer.class})
@Configuration
public class RemoteEventTargetTest {

    @Autowired
    private RemoteEventTarget testSubject;

    @Autowired
    private EventProcessorStatusUpdateSerializer serializer;

    @MockBean
    private Listener listener;

    @MockBean
    private MessagingClusterService messagingClusterService;

    @Test
    public void publishProcessorStatusUpdateEvent() {
        EventProcessorInfo processor = EventProcessorInfo
                .newBuilder()
                .setProcessorName("myProcessor")
                .build();
        ClientEventProcessorInfo payload = new ClientEventProcessorInfo("clientId", "context", processor);
        EventProcessorStatusUpdate event = new EventProcessorStatusUpdate(payload);

        ConnectorCommand command = serializer.serialize(event);
        verify(listener, times(0)).on(event);
        testSubject.accept(command);
        ArgumentCaptor<EventProcessorStatusUpdate> captor = ArgumentCaptor.forClass(EventProcessorStatusUpdate.class);
        verify(listener, times(1)).on(captor.capture());

        EventProcessorStatusUpdate value = captor.getValue();
        assertEquals(value.eventProcessorStatus().getClientName(), "clientId");
        assertEquals(value.eventProcessorStatus().getContext(), "context");
        assertEquals(value.eventProcessorStatus().getEventProcessorInfo(), processor);
    }

    private static class Listener {

        @EventListener
        public void on(EventProcessorStatusUpdate event) {
        }
    }
}