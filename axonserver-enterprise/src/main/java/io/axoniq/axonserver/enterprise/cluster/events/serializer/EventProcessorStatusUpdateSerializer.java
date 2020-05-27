package io.axoniq.axonserver.enterprise.cluster.events.serializer;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.enterprise.cluster.events.AxonServerEventSerializer;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorStatus;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.grpc.internal.ConnectorCommand.RequestCase.CLIENT_EVENT_PROCESSOR_STATUS;

/**
 * Serialization and deserialization for {@link EventProcessorStatusUpdate} internal event.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class EventProcessorStatusUpdateSerializer implements AxonServerEventSerializer<EventProcessorStatusUpdate> {

    @Override
    public Class<EventProcessorStatusUpdate> eventType() {
        return EventProcessorStatusUpdate.class;
    }

    /**
     * Serializes an {@link EventProcessorStatusUpdate} application event into a {@link ConnectorCommand}
     * where the {@link ConnectorCommand#getClientEventProcessorStatus()} field is set with a
     * {@link ClientEventProcessorStatus} representing the entire status of that Event Processor.
     *
     * @param event a {@link EventProcessorStatusUpdate} to be wrapped in a {@link ConnectorCommand} to be propagated
     *              throughout the rest of the cluster
     */
    @Override
    public ConnectorCommand serialize(EventProcessorStatusUpdate event) {
        ClientEventProcessorInfo info = event.eventProcessorStatus();
        ClientEventProcessorStatus status = ClientEventProcessorStatus.newBuilder()
                                                                      .setClient(info.getClientName())
                                                                      .setContext(info.getContext())
                                                                      .setEventProcessorInfo(info.getEventProcessorInfo())
                                                                      .build();
        return ConnectorCommand
                .newBuilder()
                .setClientEventProcessorStatus(status)
                .build();
    }

    @Override
    public ConnectorCommand.RequestCase requestCase() {
        return CLIENT_EVENT_PROCESSOR_STATUS;
    }

    /**
     * Deserializes the specified {@link ConnectorCommand} to return the corresponding {@link
     * EventProcessorStatusUpdate}
     *
     * @param message the message wrapping the content of {@link EventProcessorStatusUpdate} event
     * @return the {@link EventProcessorStatusUpdate} event
     */
    @Override
    public EventProcessorStatusUpdate deserialize(ConnectorCommand message) {
        ClientEventProcessorStatus status = message.getClientEventProcessorStatus();
        ClientEventProcessorInfo info = new ClientEventProcessorInfo(status.getClient(),
                                                                     status.getContext(),
                                                                     status.getEventProcessorInfo());
        return new EventProcessorStatusUpdate(info);
    }
}
