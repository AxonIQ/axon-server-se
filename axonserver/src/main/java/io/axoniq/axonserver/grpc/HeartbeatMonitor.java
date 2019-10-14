package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationConnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationInactivityTimeout;
import io.axoniq.axonserver.grpc.control.Heartbeat;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.message.ClientIdentification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.RequestCase.HEARTBEAT;

/**
 * Responsible for sending and receiving the heartbeat signals to and from clients.
 * Uses heartbeats to verify if the connections with clients are still alive.
 *
 * @author Sara Pellegrini
 * @since 4.2.2
 */
@Component
public class HeartbeatMonitor {

    private final Clock clock;

    private final long heartbeatTimeout;

    private final ApplicationEventPublisher eventPublisher;

    private final Publisher<PlatformOutboundInstruction> heartbeatPublisher;

    private final Map<ClientIdentification, Instant> lastReceivedHeartBeats = new ConcurrentHashMap<>();

    private final Map<ClientIdentification, String> clientComponents = new ConcurrentHashMap<>();

    /**
     * Constructs a {@link HeartbeatMonitor} that uses {@link PlatformService} to send and receive heartbeats messages.
     *
     * @param platformService  the platform service
     * @param eventPublisher   the internal event publisher
     * @param heartbeatTimeout the max period of inactivity before is published an {@link ApplicationInactivityTimeout};
     *                         it is expressed in milliseconds
     */
    @Autowired
    public HeartbeatMonitor(PlatformService platformService,
                            ApplicationEventPublisher eventPublisher,
                            @Value("${axoniq.axonserver.client-heartbeat-timeout:5000}") long heartbeatTimeout) {
        this(listener ->
                     platformService.onInboundInstruction(HEARTBEAT, (client, context, instruction) ->
                             listener.accept(new ClientIdentification(context, client), instruction)),
             eventPublisher,
             platformService::sendToAllClients,
             heartbeatTimeout, Clock.systemUTC());
    }

    /**
     * Primary constructor of {@link HeartbeatMonitor}
     *
     * @param heartbeatListenerRegistration consumers of heartbeats listener used to register a listener
     *                                      for the heartbeats received from clients
     * @param eventPublisher                the internal event publisher
     * @param heartbeatPublisher            the heartbeat publisher
     * @param heartbeatTimeout              the max period of inactivity before is published an {@link
     *                                      ApplicationInactivityTimeout}; it is expressed in milliseconds
     * @param clock                         the clock
     */
    public HeartbeatMonitor(
            Consumer<BiConsumer<ClientIdentification, PlatformInboundInstruction>> heartbeatListenerRegistration,
            ApplicationEventPublisher eventPublisher,
            Publisher<PlatformOutboundInstruction> heartbeatPublisher,
            long heartbeatTimeout, Clock clock) {
        this.clock = clock;
        heartbeatListenerRegistration.accept(this::onHeartBeat);
        this.eventPublisher = eventPublisher;
        this.heartbeatTimeout = heartbeatTimeout;
        this.heartbeatPublisher = heartbeatPublisher;
    }

    private void onHeartBeat(ClientIdentification clientIdentification, PlatformInboundInstruction heartbeat) {
        lastReceivedHeartBeats.put(clientIdentification, Instant.now(clock));
    }

    /**
     * Collects components when application connect to AxonServer.
     * @param evt the connection event
     */
    @EventListener
    public void on(ApplicationConnected evt) {
        ClientIdentification clientIdentification = new ClientIdentification(evt.getContext(), evt.getClient());
        clientComponents.put(clientIdentification, evt.getComponentName());
    }

    /**
     * Checks if the connections are still alive, if not publish an {@link ApplicationInactivityTimeout} event.
     */
    @Scheduled(initialDelay = 10_000, fixedRateString = "${axoniq.axonserver.client-heartbeat-check-rate:1000}")
    public void checkClientsStillAlive() {
        Instant timeout = Instant.now(clock).minus(heartbeatTimeout, ChronoUnit.MILLIS);
        lastReceivedHeartBeats.forEach((client, instant) -> {
            if (instant.isBefore(timeout) && clientComponents.containsKey(client)) {
                String component = clientComponents.get(client);
                eventPublisher.publishEvent(new ApplicationInactivityTimeout(client, component));
            }
        });
    }


    /**
     * Sends an heartbeat signal every 500 milliseconds.
     */
    @Scheduled(initialDelay = 5_000, fixedRateString = "${axoniq.axonserver.client-heartbeat-frequency:500}")
    public void sendHeartbeat() {
        heartbeatPublisher.publish(PlatformOutboundInstruction
                                           .newBuilder()
                                           .setHeartbeat(Heartbeat.newBuilder())
                                           .build());
    }


    /**
     * Clears last heartbeat received from a client that disconnects from AxonServer.
     *
     * @param evt the disconnection event
     */
    @EventListener
    public void on(ApplicationDisconnected evt) {
        ClientIdentification clientIdentification = new ClientIdentification(evt.getContext(), evt.getClient());
        lastReceivedHeartBeats.remove(clientIdentification);
        clientComponents.remove(clientIdentification);
    }
}
