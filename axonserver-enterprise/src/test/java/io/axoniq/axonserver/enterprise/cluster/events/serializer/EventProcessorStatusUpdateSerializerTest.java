package io.axoniq.axonserver.enterprise.cluster.events.serializer;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorStatus;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link EventProcessorStatusUpdateSerializer}.
 *
 * @author Sara Pellegrini
 */
public class EventProcessorStatusUpdateSerializerTest {

    private final EventProcessorInfo processorInfo = EventProcessorInfo
            .newBuilder()
            .setProcessorName("P")
            .setMode("Mode")
            .setActiveThreads(5)
            .setRunning(true)
            .setError(true)
            .build();

    EventProcessorStatusUpdateSerializer testSubject = new EventProcessorStatusUpdateSerializer();

    @Test
    public void serialize() {
        ClientEventProcessorInfo clientEventProcessorInfo = new ClientEventProcessorInfo("myClient",
                                                                                         "myContext",
                                                                                         processorInfo);
        EventProcessorStatusUpdate event = new EventProcessorStatusUpdate(clientEventProcessorInfo);
        ConnectorCommand message = testSubject.serialize(event);
        EventProcessorStatusUpdate deserialized = testSubject.deserialize(message);
        assertEquals(event.eventProcessorStatus().getContext(), deserialized.eventProcessorStatus().getContext());
        assertEquals(event.eventProcessorStatus().getClientName(), deserialized.eventProcessorStatus().getClientName());
        assertEquals(event.eventProcessorStatus().getEventProcessorInfo(),
                     deserialized.eventProcessorStatus().getEventProcessorInfo());
    }

    @Test
    public void deserialize() {

        ConnectorCommand message = ConnectorCommand.newBuilder()
                                                   .setClientEventProcessorStatus(
                                                           ClientEventProcessorStatus
                                                                   .newBuilder()
                                                                   .setClient("myClient")
                                                                   .setContext("myContext")
                                                                   .setEventProcessorInfo(processorInfo))
                                                   .build();
        EventProcessorStatusUpdate event = testSubject.deserialize(message);
        ConnectorCommand serializedMessage = testSubject.serialize(event);
        assertEquals(message, serializedMessage);
    }
}