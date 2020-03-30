/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.instance;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.message.ClientIdentification;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache for connected client applications.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Primary @Component
public class GenericClients implements Clients {

    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final Map<ClientIdentification, Client> clientRegistrations = new ConcurrentHashMap<>();

    public GenericClients(MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @Override
    public Iterator<Client> iterator() {
        return clientRegistrations.values().iterator();
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        this.clientRegistrations.remove(event.clientIdentification());
    }

    @EventListener
    public void on(TopologyEvents.ApplicationConnected event) {
        this.clientRegistrations.put(event.clientIdentification(),
                                     new GenericClient(event.getClient(),
                                                       event.getComponentName(),
                                                       event.getContext(),
                                                       event.isProxied() ? event.getProxy() : messagingPlatformConfiguration
                                                               .getName()));
    }
}
