package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.function.BiConsumer;

/**
 * Publisher of heartbeat pulses, that is responsible to send heartbeat only to clients that support this feature.
 *
 * @author Sara Pellegrini
 * @since 4.2.3
 */
@Component
public class HeartbeatPublisher implements Publisher<PlatformOutboundInstruction> {

    private final Clients clientsSupportingHeartbeat;

    private final BiConsumer<String, PlatformOutboundInstruction> clientPublisher;

    /**
     * Constructs a {@link HeartbeatPublisher} that uses the {@link PlatformService} to send a gRPC heartbeat message to
     * the clients which supports heartbeat feature, defined by {@link HeartbeatProvidedClients}.
     *
     * @param clients         clients which support heartbeat feature
     * @param platformService used to send heartbeat as gRPC message
     */
    @Autowired
    public HeartbeatPublisher(HeartbeatProvidedClients clients,
                              PlatformService platformService) {
        this(clients, platformService::sendToClient);
    }

    /**
     * Constructs a {@link HeartbeatPublisher} that sends heartbeats to specified {@link Clients} supporting this
     * feature.
     *
     * @param clientsSupportingHeartbeat clients which support heartbeat feature
     * @param clientPublisher publisher used to send the heartbeat pulse to a single client
     */
    public HeartbeatPublisher(Clients clientsSupportingHeartbeat,
                              BiConsumer<String, PlatformOutboundInstruction> clientPublisher) {
        this.clientsSupportingHeartbeat = clientsSupportingHeartbeat;
        this.clientPublisher = clientPublisher;
    }

    /**
     * Publish an instruction to the clients supporting heartbeat feature.
     *
     * @param heartbeat the heartbeat instruction
     */
    public void publish(PlatformOutboundInstruction heartbeat) {
        for (Client client : clientsSupportingHeartbeat) {
            try {
                clientPublisher.accept(client.name(), heartbeat);
            } catch (RuntimeException ignore) {
                // failing to send heartbeat can be ignored
            }
        }
    }
}
