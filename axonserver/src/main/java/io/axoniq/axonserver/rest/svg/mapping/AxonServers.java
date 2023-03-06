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
import io.axoniq.axonserver.topology.Topology;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class AxonServers implements Function<Predicate<String>, Stream<AxonServer>> {

    private static final String ADMIN = "_admin";

    private final Topology topology;

    public AxonServers(Topology topology) {
        this.topology = topology;
    }

    @Override
    @Nonnull
    public Stream<AxonServer> apply(Predicate<String> contextFilter) {
        return topology.nodes()
                       .filter(n -> n.getContextNames().stream().anyMatch(contextFilter::test))
                       .sorted(Comparator.comparing(AxonServerNode::getName))
                       .map(node -> (AxonServer) new AxonServer() {

                           @Override
                           public boolean isActive() {
                               return topology.isActive(node);
                           }

                           @Override
                           public AxonServerNode node() {
                               return node;
                           }

                                     @Override
                                     public List<String> contexts() {
                                         return node.getContextNames().stream()
                                                    .filter(contextFilter::test)
                                                    .sorted().collect(
                                                         Collectors.toList());
                                     }

                                     @Override
                                     public List<Storage> storage() {
                                         return node.getStorageContextNames()
                                                    .stream()
                                                    .filter(contextFilter::test)
                                                    .map(contextName -> new Storage() {
                                                        @Override
                                                        public String context() {
                                                            return contextName;
                                                        }

                                                        @Override
                                                        public boolean master() {
                                                            return topology.isLeader(node.getName(), contextName);
                                                        }
                                                    }).sorted(Comparator.comparing(Storage::context)).collect(Collectors
                                                                                                                      .toList());
                                     }

                           @Override
                           public Map<String, String> tags() {
                               return node.getTags();
                           }

                           @Override
                           public boolean isAdminLeader() {
                               return topology.isLeader(node.getName(),
                                                        ADMIN);
                           }
                       });
    }
}
