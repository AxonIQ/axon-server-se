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
 * @author Sara Pellegrini
 * @since 4.2.3
 */
@Component
public class HeartbeatPublisher implements Publisher<PlatformOutboundInstruction> {

    private final Clients clientsSupportingHeartbeat;

    private final BiConsumer<String, PlatformOutboundInstruction> clientPublisher;

    @Autowired
    public HeartbeatPublisher(HeartbeatProvidedClients clients,
                              PlatformService platformService) {
        this(clients, platformService::sendToClient);
    }

    public HeartbeatPublisher(Clients clientsSupportingHeartbeat,
                              BiConsumer<String, PlatformOutboundInstruction> clientPublisher) {
        this.clientsSupportingHeartbeat = clientsSupportingHeartbeat;
        this.clientPublisher = clientPublisher;
    }

    public void publish(PlatformOutboundInstruction heartbeat) {
        for (Client client : clientsSupportingHeartbeat) {
            clientPublisher.accept(client.name(), heartbeat);
        }
    }
}
