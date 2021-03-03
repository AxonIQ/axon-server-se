/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.mapping;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static java.util.stream.Collectors.toSet;

/**
 * Created by Sara Pellegrini on 01/05/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class Applications implements Function<String, Stream<Application>> {

    private final Topology clusterController;

    private final Map<ComponentContext, Set<ConnectedClient>> clientsPerComponent = new ConcurrentHashMap<>();

    public Applications(Topology clusterController) {
        this.clusterController = clusterController;
    }


    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        clientsPerComponent.forEach((k, v) -> {
            if (k.context.equals(event.getContext())) {
                v.remove(new ConnectedClient(event.getClientStreamId(), null));
            }
        });
        clientsPerComponent.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    @EventListener
    public void on(TopologyEvents.ApplicationConnected event) {
        clientsPerComponent.forEach((component, clients) -> {
            if (component.context.equals(event.getContext())) {
                clients.removeIf(c -> c.client.equals(event.getClientStreamId()));
            }
        });
        clientsPerComponent.computeIfAbsent(new ComponentContext(event.getComponentName(), event.getContext()),
                                            k -> new CopyOnWriteArraySet<>())
                           .add(new ConnectedClient(event.getClientStreamId(),
                                                    event.isProxied() ? event.getProxy() : getCurrentNode()));
    }

    private String getCurrentNode() {
        return clusterController.getName();
    }


    @Override
    @Nonnull
    public Stream<Application> apply(String context) {
        List<Map.Entry<ComponentContext, Set<ConnectedClient>>> sortedComponents = clientsPerComponent.entrySet()
                                                                                                      .stream()
                                                                                                      .filter(c -> context
                                                                                                              == null
                                                                                                              || c
                                                                                                              .getKey().context
                                                                                                              .equals(context))
                                                                                                      .filter(c -> clusterController
                                                                                                              .validContext(
                                                                                                                      c.getKey().context))
                                                                                                      .sorted(
                                                                                                              (o1, o2) -> {
                                                                                                                  ConnectedClient client1 = o1
                                                                                                                          .getValue()
                                                                                                                          .stream()
                                                                                                                          .min(Comparator
                                                                                                                                       .comparing(
                                                                                                                                               v -> v.axonHubServer))
                                                                                                                          .orElse(new ConnectedClient(
                                                                                                                                  "",
                                                                                                                                  "ZZZZ"));
                    ConnectedClient client2 = o2.getValue().stream().min(Comparator.comparing(v -> v.axonHubServer))
                                                              .orElse(new ConnectedClient("", "ZZZZ"));
                    int v = client1.axonHubServer.compareTo(client2.axonHubServer);
                    if (v == 0) {
                        return o1.getKey().compareTo(o2.getKey());
                    }
                    return v;
                }).collect(Collectors.toList());

        return sortedComponents.stream().map(entry -> (Application) new Application() {
            @Override
            public String name() {
                return entry.getKey().toString(clusterController.isMultiContext());
            }

            @Override
            public String component() {
                return entry.getKey().component;
            }

            @Override
            public String context() {
                return entry.getKey().context;
            }

            @Override
            public int instances() {
                return entry.getValue().size();
            }

            @Override
            public Iterable<String> connectedHubNodes() {
                return entry.getValue().stream().map(client -> client.axonHubServer).collect(toSet());
            }
        });
    }

    private static class ComponentContext implements Comparable<ComponentContext>{

        private final String component;
        private final String context;

        private ComponentContext(String component, String context) {
            this.component = component;
            this.context = context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ComponentContext that = (ComponentContext) o;
            return Objects.equals(component, that.component) &&
                    Objects.equals(context, that.context);
        }

        @Override
        public int hashCode() {

            return Objects.hash(component, context);
        }

        @Override
        public int compareTo(@Nonnull ComponentContext other) {
            if( other.component.equals(component)) return context.compareTo(other.context);
            return component.compareTo(other.component);
        }

        String toString(boolean isMultiContext) {
            if( isMultiContext) return component + "@" + context;
            return component;
        }
    }
    private static class ConnectedClient  {
        final String client;
        final String axonHubServer;

        private ConnectedClient(String client, String axonHubServer) {
            this.client = client;
            this.axonHubServer = axonHubServer;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConnectedClient that = (ConnectedClient) o;
            return Objects.equals(client, that.client);
        }

        @Override
        public int hashCode() {
            return Objects.hash(client);
        }

    }
}
