/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.mapping;

import io.axoniq.axonserver.topology.AxonServerNode;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class AxonServers implements Iterable<AxonServer> {

    private final Topology clusterController;
    private final EventStoreLocator eventStoreManager;

    public AxonServers(Topology clusterController,
                       EventStoreLocator eventStoreManager) {
        this.clusterController = clusterController;
        this.eventStoreManager = eventStoreManager;
    }

    @Override
    @Nonnull
    public Iterator<AxonServer> iterator() {
        return  clusterController.nodes()
                                 .sorted(Comparator.comparing(AxonServerNode::getName))
                                 .map(node -> (AxonServer) new AxonServer() {

                                     @Override
                                     public boolean isActive() {
                                         return clusterController.isActive(node);
                                     }

                                     @Override
                                     public AxonServerNode node() {
                                         return node;
                                     }

                                     @Override
                                     public List<String> contexts() {
                                         return node.getContextNames().stream().sorted().collect(
                                                 Collectors.toList());
                                     }

                                     @Override
                                     public List<Storage> storage() {
                                         return node.getStorageContextNames().stream().map(contextName -> new Storage() {
                                             @Override
                                             public String context() {
                                                 return contextName;
                                             }

                                             @Override
                                             public boolean master() {
                                                 return eventStoreManager.isLeader(node.getName(), contextName);
                                             }
                                         } ).sorted(Comparator.comparing(Storage::context)).collect(Collectors.toList());
                                     }
                                 }).iterator();
    }
}
