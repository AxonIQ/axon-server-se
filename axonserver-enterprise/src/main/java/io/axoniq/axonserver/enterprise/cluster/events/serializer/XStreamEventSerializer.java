package io.axoniq.axonserver.enterprise.cluster.events.serializer;

import com.google.protobuf.ByteString;
import com.thoughtworks.xstream.XStream;
import io.axoniq.axonserver.applicationevents.AxonServerEvent;
import io.axoniq.axonserver.enterprise.cluster.events.AxonServerEventSerializer;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import org.springframework.stereotype.Component;

/**
 * Serializer for internal events based on {@link XStream}.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class XStreamEventSerializer implements AxonServerEventSerializer<AxonServerEvent> {

    private final XStream xStream = new XStream();

    @Override
    public Class<AxonServerEvent> eventType() {
        return AxonServerEvent.class;
    }

    /**
     * Serialize an object into a {@link SerializedObject} using {@link XStream}.
     *
     * @param event the event to serialize
     * @return the {@link ConnectorCommand} wrapping the serialized event
     */
    @Override
    public ConnectorCommand serialize(AxonServerEvent event) {
        try {
            ByteString data = ByteString.copyFrom(xStream.toXML(event).getBytes());
            SerializedObject serializedObject = SerializedObject.newBuilder()
                                                                .setData(data)
                                                                .setType(event.getClass().getName())
                                                                .build();
            return ConnectorCommand.newBuilder().setInternalEvent(serializedObject).build();
        } catch (Throwable e) {
            throw new RuntimeException(String.format("Impossible to serialize the object: %s", event), e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @return {@link ConnectorCommand.RequestCase#INTERNAL_EVENT}
     */
    @Override
    public ConnectorCommand.RequestCase requestCase() {
        return ConnectorCommand.RequestCase.INTERNAL_EVENT;
    }

    /**
     * Deserialize a {@link ConnectorCommand} containing a generic internal event using {@link XStream}.
     *
     * @param connectorCommand the {@link ConnectorCommand} wrapping the internal event to deserialize
     * @return the de-serialized event
     */
    @Override
    public AxonServerEvent deserialize(ConnectorCommand connectorCommand) {
        SerializedObject serializedObject = connectorCommand.getInternalEvent();
        try {
            return (AxonServerEvent) xStream.fromXML(new String(serializedObject.getData().toByteArray()));
        } catch (Throwable e) {
            throw new RuntimeException(String.format("Impossible to deserialize the object: %s", serializedObject), e);
        }
    }
}
