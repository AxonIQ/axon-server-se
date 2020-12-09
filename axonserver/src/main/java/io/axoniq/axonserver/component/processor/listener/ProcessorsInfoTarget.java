/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.listener;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdated;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.util.TimeLimitedCache;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class ProcessorsInfoTarget implements ClientProcessors {

    // Map<ClientStreamId,Component>
    private final Map<String, String> clients = new ConcurrentHashMap<>();

    // Map<ClientStreamId, Map<ProcessorName, ClientProcessor>>
    private final Map<String, TimeLimitedCache<String, ClientProcessor>> cache = new ConcurrentHashMap<>();

    private final ClientProcessorMapping mapping;
    private final long expireTime;
    private final Clock clock;

    public ProcessorsInfoTarget(Clock clock,
                                @Value("${axoniq.axonserver.processor-info-timeout:30000}") long expireTime) {
        this.clock = clock;
        this.mapping = new ClientProcessorMapping() {
        };
        this.expireTime = expireTime;
    }

    @EventListener
    @Order(0)
    public EventProcessorStatusUpdated onEventProcessorStatusChange(EventProcessorStatusUpdate event) {
        ClientEventProcessorInfo processorStatus = event.eventProcessorStatus();
        String clientId = processorStatus.getClientId();
        String clientStreamId = processorStatus.getClientStreamId();
        TimeLimitedCache<String, ClientProcessor> clientData = cache.computeIfAbsent(clientStreamId,
                                                                                     c -> new TimeLimitedCache<>(clock,
                                                                                                                 expireTime));
        EventProcessorInfo eventProcessorInfo = processorStatus.getEventProcessorInfo();
        ClientProcessor clientProcessor = mapping.map(clientId,
                                                      clients.get(clientStreamId),
                                                      processorStatus.getContext(),
                                                      eventProcessorInfo);
        clientData.put(eventProcessorInfo.getProcessorName(), clientProcessor);
        return new EventProcessorStatusUpdated(processorStatus, false);
    }

    @EventListener
    @Order(0)
    public void onClientConnected(TopologyEvents.ApplicationConnected event) {
        clients.put(event.getClientStreamId(), event.getComponentName());
        cleanupCache();
    }

    @EventListener
    @Order(0)
    public void onClientDisconnected(TopologyEvents.ApplicationDisconnected event) {
        clients.remove(event.getClientStreamId());
        cache.remove(event.getClientStreamId());
        cleanupCache();
    }

    @Nonnull
    @Override
    public Iterator<ClientProcessor> iterator() {
        return cache.entrySet().stream()
                    .flatMap(client -> client.getValue().values().stream())
                    .iterator();
    }

    private void cleanupCache() {
        cache.entrySet().removeIf(e -> e.getValue().isEmpty());
    }
}
