/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.component.instance.ClientIdentifications;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Publisher of heartbeat pulses, that is responsible to send heartbeat only to clients that support this feature.
 *
 * @author Sara Pellegrini
 * @since 4.2.3
 */
@Component
public class HeartbeatPublisher implements Publisher<PlatformOutboundInstruction> {

    private final Logger logger = LoggerFactory.getLogger(HeartbeatPublisher.class);
    private final ClientIdentifications clientsSupportingHeartbeat;
    private final ClientPublisher clientPublisher;

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
        this(clients, platformService::sendToClientStreamId);
    }

    /**
     * Constructs a {@link HeartbeatPublisher} that sends heartbeats to specified {@link Clients} supporting this
     * feature.
     *
     * @param clientsSupportingHeartbeat clients which support heartbeat feature
     * @param clientPublisher            publisher used to send the heartbeat pulse to a single client
     */
    public HeartbeatPublisher(ClientIdentifications clientsSupportingHeartbeat,
                              ClientPublisher clientPublisher) {
        this.clientsSupportingHeartbeat = clientsSupportingHeartbeat;
        this.clientPublisher = clientPublisher;
    }

    /**
     * Publish an instruction to the clients supporting heartbeat feature.
     *
     * @param heartbeat the heartbeat instruction
     */
    public void publish(PlatformOutboundInstruction heartbeat) {
        for (ClientStreamIdentification client : clientsSupportingHeartbeat) {
            try {
                clientPublisher.publish(client.getClientStreamId(), heartbeat);
                logger.trace("HeartBeat sent to client {}", client);
            } catch (RuntimeException ignore) {
                // failing to send heartbeat can be ignored
            }
        }
    }

    @FunctionalInterface
    public interface ClientPublisher {

        /**
         * Publishes an {@link PlatformOutboundInstruction} to a single client
         *
         * @param clientStreamId the platform stream identifier of the client
         * @param instruction    the {@link PlatformOutboundInstruction} to be sent
         */
        void publish(String clientStreamId, PlatformOutboundInstruction instruction);
    }
}
