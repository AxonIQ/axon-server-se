/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationConnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationInactivityTimeout;
import io.axoniq.axonserver.grpc.ClientContext;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.control.Heartbeat;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
@ConditionalOnProperty(value = "axoniq.axonserver.heartbeat.enabled")
public class HeartbeatMonitor {

    private final Logger logger = LoggerFactory.getLogger(HeartbeatMonitor.class);

    private final Map<ClientStreamIdentification, ClientInformation> clientInfos = new ConcurrentHashMap<>();

    private final Clock clock;

    private final long heartbeatTimeout;

    private final ApplicationEventPublisher eventPublisher;

    private final Publisher<PlatformOutboundInstruction> heartbeatPublisher;

    private final Map<ClientStreamIdentification, Instant> lastReceivedHeartBeats = new ConcurrentHashMap<>();

    /**
     * Collects components when application connect to AxonServer.
     *
     * @param evt the connection event
     */
    @EventListener
    public void on(ApplicationConnected evt) {
        ClientStreamIdentification clientIdentification = new ClientStreamIdentification(evt.getContext(),
                                                                                         evt.getClientStreamId());
        clientInfos.put(clientIdentification, new ClientInformation(evt.getComponentName(), evt.getClientId(), evt.getContext()));
    }

    /**
     * Constructs a {@link HeartbeatMonitor} that uses {@link PlatformService} to send and receive heartbeats messages.
     *
     * @param platformService    the platform service
     * @param heartbeatPublisher the heartbeat publisher
     * @param eventPublisher     the internal event publisher
     * @param heartbeatTimeout   the max period of inactivity before is published an {@link ApplicationInactivityTimeout};
     *                           it is expressed in milliseconds
     */
    @Autowired
    public HeartbeatMonitor(PlatformService platformService,
                            HeartbeatPublisher heartbeatPublisher,
                            ApplicationEventPublisher eventPublisher,
                            @Value("${axoniq.axonserver.client-heartbeat-timeout:5000}") long heartbeatTimeout) {
        this(listener ->
                     platformService.onInboundInstruction(HEARTBEAT, (clientComponent, instruction) ->
                             listener.accept(new ClientStreamIdentification(clientComponent.getContext(),
                                                                            clientComponent.getClientStreamId()),
                                             instruction)),
             eventPublisher,
             heartbeatPublisher,
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
            Consumer<BiConsumer<ClientStreamIdentification, PlatformInboundInstruction>> heartbeatListenerRegistration,
            ApplicationEventPublisher eventPublisher,
            Publisher<PlatformOutboundInstruction> heartbeatPublisher,
            long heartbeatTimeout, Clock clock) {
        heartbeatListenerRegistration.accept(this::onHeartBeat);
        this.clock = clock;
        this.eventPublisher = eventPublisher;
        this.heartbeatTimeout = heartbeatTimeout;
        this.heartbeatPublisher = heartbeatPublisher;
    }

    private void onHeartBeat(ClientStreamIdentification clientIdentification, PlatformInboundInstruction heartbeat) {
        logger.trace("HeartBeat received from {}", clientIdentification);
        lastReceivedHeartBeats.put(clientIdentification, Instant.now(clock));
    }

    /**
     * Checks if the connections are still alive, if not publish an {@link ApplicationInactivityTimeout} event.
     */
    @Scheduled(initialDelayString = "${axoniq.axonserver.client-heartbeat-check-initial-delay:10000}",
            fixedRateString = "${axoniq.axonserver.client-heartbeat-check-rate:1000}")
    public void checkClientsStillAlive() {
        logger.debug("Checking connected clients are still alive...");
        Instant timeout = Instant.now(clock).minus(heartbeatTimeout, ChronoUnit.MILLIS);
        lastReceivedHeartBeats.forEach((clientStreamIdentification, instant) -> {
            if (instant.isBefore(timeout) && clientInfos.containsKey(clientStreamIdentification)) {

                ClientInformation clientInformation = clientInfos.get(clientStreamIdentification);
                String component = clientInformation.component;
                String clientId = clientInformation.clientId;
                logger.info(
                        "Client inactivity detected for more than {} milliseconds. Client: {}, lastActivity: {}, timeout: {}. ",
                        heartbeatTimeout,
                        clientStreamIdentification,
                        instant,
                        timeout);
                eventPublisher.publishEvent(new ApplicationInactivityTimeout(clientStreamIdentification, component,
                                                                             new ClientContext(clientId,
                                                                                               clientInformation.context)));
            }
        });
    }

    /**
     * Clears last heartbeat received from a client that disconnects from AxonServer.
     *
     * @param evt the disconnection event
     */
    @EventListener
    public void on(ApplicationDisconnected evt) {
        ClientStreamIdentification clientIdentification = new ClientStreamIdentification(evt.getContext(),
                                                                                         evt.getClientStreamId());
        lastReceivedHeartBeats.remove(clientIdentification);
        clientInfos.remove(clientIdentification);
        logger.debug("Stop monitoring heartbeat for client {}.", clientIdentification);
    }


    /**
     * Sends an heartbeat signal every 500 milliseconds.
     */
    @Scheduled(initialDelayString = "${axoniq.axonserver.client-heartbeat-initial-delay:5000}",
            fixedRateString = "${axoniq.axonserver.client-heartbeat-frequency:500}")
    public void sendHeartbeat() {
        heartbeatPublisher.publish(PlatformOutboundInstruction
                                           .newBuilder()
                                           .setHeartbeat(Heartbeat.newBuilder())
                                           .build());
    }

    private static final class ClientInformation {

        private final String component;
        private final String clientId;
        private final String context;

        private ClientInformation(String component, String clientId, String context) {
            this.component = component;
            this.clientId = clientId;
            this.context = context;
        }
    }
}
