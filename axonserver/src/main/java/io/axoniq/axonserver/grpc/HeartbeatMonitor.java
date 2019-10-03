package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationConnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationInactivityTimeout;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.message.ClientIdentification;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.RequestCase.HEARTBEAT;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
@Component
public class HeartbeatMonitor {

    private final long heartbeatTimeout;

    private final ApplicationEventPublisher eventPublisher;

    private final Map<ClientIdentification, Instant> lastReceivedHeartBeats = new ConcurrentHashMap<>();

    private final Map<ClientIdentification, String> clientComponents = new ConcurrentHashMap<>();

    public HeartbeatMonitor(PlatformService platformService,
                            ApplicationEventPublisher eventPublisher,
                            @Value("${axoniq.axonserver.client-heartbeat-timeout:5000}") long heartbeatTimeout) {
        platformService.onInboundInstruction(HEARTBEAT, this::onHeartBeat);
        this.eventPublisher = eventPublisher;
        this.heartbeatTimeout = heartbeatTimeout;
    }

    private void onHeartBeat(String client, String context, PlatformInboundInstruction heartbeat) {
        ClientIdentification clientIdentification = new ClientIdentification(context, client);
        lastReceivedHeartBeats.put(clientIdentification, Instant.now());
    }

    @EventListener
    public void on(ApplicationConnected evt) {
        ClientIdentification clientIdentification = new ClientIdentification(evt.getContext(), evt.getClient());
        clientComponents.put(clientIdentification, evt.getComponentName());
    }

    @Scheduled(initialDelay = 10_000, fixedDelay = 1_000)
    public void checkClientsStillAlive() {
        Instant timeout = Instant.now().minus(heartbeatTimeout, ChronoUnit.MILLIS);
        lastReceivedHeartBeats.forEach((client, instant) -> {
            if (instant.isBefore(timeout) && clientComponents.containsKey(client)) {
                String component = clientComponents.get(client);
                eventPublisher.publishEvent(new ApplicationInactivityTimeout(client, component));
            }
        });
    }

    @EventListener
    public void on(ApplicationDisconnected evt) {
        ClientIdentification clientIdentification = new ClientIdentification(evt.getContext(), evt.getClient());
        lastReceivedHeartBeats.remove(clientIdentification);
        clientComponents.remove(clientIdentification);
    }
}
