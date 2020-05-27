package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.applicationevents.AxonServerEvent;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;

/**
 * Provides serialization and deserialization for specific {@link AxonServerEvent}, in order to send it to other
 * Axon Server instances wrapped in a {@link ConnectorCommand} message.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public interface AxonServerEventSerializer<Event extends AxonServerEvent> {

    /**
     * Returns the event type that this instance is able to serialize into a {@link ConnectorCommand}. In other words,
     * any object that is instance of this type should be serializable by this {@link AxonServerEventSerializer}.
     *
     * @return the event type that this instance is able to serialize.
     */
    Class<Event> eventType();

    /**
     * Serializes the {@link Event} into a {@link ConnectorCommand} message
     *
     * @param event the {@link Event} to be serialized
     * @return the {@link ConnectorCommand} message wrapping the serialized event
     */
    ConnectorCommand serialize(Event event);

    /**
     * Returns the {@link io.axoniq.axonserver.grpc.internal.ConnectorCommand.RequestCase}
     * of the {@link ConnectorCommand} that this instance is able to deserialize.
     *
     * @return the {@link io.axoniq.axonserver.grpc.internal.ConnectorCommand.RequestCase}
     */
    ConnectorCommand.RequestCase requestCase();

    /**
     * Deserializes the specified {@link ConnectorCommand} into a new {@link Event}
     *
     * @param connectorCommand the {@link ConnectorCommand} to deserialize
     * @return the de-serialized {@link Event}
     */
    Event deserialize(ConnectorCommand connectorCommand);
}
