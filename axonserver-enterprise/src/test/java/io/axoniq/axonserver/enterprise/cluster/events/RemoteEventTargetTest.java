package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.enterprise.cluster.events.serializer.EventProcessorStatusUpdateSerializer;
import io.axoniq.axonserver.enterprise.cluster.internal.MessagingClusterService;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for {@link RemoteEventReceiver}
 *
 * @author Sara Pellegrini
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {RemoteEventReceiver.class, EventProcessorStatusUpdateSerializer.class})
@Configuration
public class RemoteEventTargetTest {

    @Autowired
    @Qualifier("applicationEventPublisher")
    private ApplicationEventPublisher localPublisher;

    @Autowired
    private List<AxonServerEventSerializer<?>> serializers;

    @Autowired
    private EventProcessorStatusUpdateSerializer serializer;

    @MockBean
    private Listener listener;

    @MockBean
    private MessagingClusterService messagingClusterService;

    @Test
    public void publishProcessorStatusUpdateEvent() {
        AtomicReference<Consumer<ConnectorCommand>> commandHandler = new AtomicReference<>();
        doAnswer(invocation -> {
            commandHandler.set(invocation.getArgument(1));
            return (Registration) () -> commandHandler.set(null);
        }).when(messagingClusterService).registerConnectorCommandHandler(any(), any());

        new RemoteEventReceiver(localPublisher,
                                serializers,
                                messagingClusterService);

        EventProcessorInfo processor = EventProcessorInfo
                .newBuilder()
                .setProcessorName("myProcessor")
                .build();
        ClientEventProcessorInfo payload = new ClientEventProcessorInfo("clientId", "context", processor);
        EventProcessorStatusUpdate event = new EventProcessorStatusUpdate(payload);

        ConnectorCommand command = serializer.serialize(event);
        verify(listener, times(0)).on(event);
        commandHandler.get().accept(command);
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