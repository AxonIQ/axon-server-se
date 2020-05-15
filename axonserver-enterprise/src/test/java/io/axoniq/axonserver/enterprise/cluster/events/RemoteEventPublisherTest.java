package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.applicationevents.AxonServerEvent;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.enterprise.cluster.events.serializer.EventProcessorStatusUpdateSerializer;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link RemoteEventPublisher}.
 *
 * @author Sara Pellegrini
 */
public class RemoteEventPublisherTest {

    private final EventProcessorStatusUpdateSerializer serializer = new EventProcessorStatusUpdateSerializer();
    private List<ConnectorCommand> publishedMessages = new ArrayList<>();
    private RemoteEventPublisher testSubject = new RemoteEventPublisher(publishedMessages::add, asList(serializer));

    @Before
    public void setUp() throws Exception {
        publishedMessages.clear();
    }

    @Test
    public void publish() {
        ClientEventProcessorInfo info = new ClientEventProcessorInfo("client",
                                                                     "context",
                                                                     EventProcessorInfo.newBuilder().build());
        EventProcessorStatusUpdate event = new EventProcessorStatusUpdate(info);
        testSubject.publish(event);

        assertFalse(publishedMessages.isEmpty());
        assertEquals(serializer.serialize(event), publishedMessages.get(0));
    }

    @Test(expected = RuntimeException.class)
    public void publishWithoutSerializer() {
        class MyEvent implements AxonServerEvent {

        }
        MyEvent event = new MyEvent();
        testSubject.publish(event);
    }
}