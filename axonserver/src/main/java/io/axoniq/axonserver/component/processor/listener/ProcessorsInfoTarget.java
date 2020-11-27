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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class ProcessorsInfoTarget implements ClientProcessors {

    // Map<ClientStreamId,Component>
    private final Map<String, String> clients = new ConcurrentHashMap<>();

    // Map<ClientStreamId, Map<ProcessorName, ClientProcessor>>
    private final Map<String, Map<String, ClientProcessor>> cache = new ConcurrentHashMap<>();

    private final ClientProcessorMapping mapping;
    private final long expireTime;

    public ProcessorsInfoTarget(@Value("${axoniq.axonserver.processor-info-timeout:30000}") long expireTime) {
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
        Map<String, ClientProcessor> clientData = cache.computeIfAbsent(clientStreamId, c -> new HashMap<>());
        EventProcessorInfo eventProcessorInfo = processorStatus.getEventProcessorInfo();
        ClientProcessor clientProcessor = mapping.map(clientId,
                                                      clients.get(clientStreamId),
                                                      processorStatus.getContext(),
                                                      eventProcessorInfo,
                                                      processorStatus.getTimestamp());
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

    private void cleanupCache() {
        long expired = System.currentTimeMillis() - expireTime;
        cache.values().forEach(map -> map.entrySet().removeIf(e -> e.getValue().timestamp() < expired));
    }

    @Nonnull
    @Override
    public Iterator<ClientProcessor> iterator() {
        long expired = System.currentTimeMillis() - expireTime;
        return cache.entrySet().stream()
                    .flatMap(client -> client.getValue().values().stream())
                    .filter(processor -> processor.timestamp() >= expired)
                    .iterator();
    }
}
