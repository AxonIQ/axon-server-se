/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.commandprocesing.imp.InMemoryCommandHandlerRegistry;
import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerRegistry;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.command.CommandMetricsRegistry;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.message.query.RoundRobinQueryHandlerSelector;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Marc Gathier
 */
public class MetricsRestControllerTest {

    private MetricsRestController testSubject;
    private CommandMetricsRegistry commandMetricsRegistry;
    private QueryMetricsRegistry queryMetricsRegistry;
    private ClientStreamIdentification testclient;
    private ClientStreamIdentification queryClient;
    private Principal principal;

    @Before
    public void setUp() {
        CommandHandlerRegistry commandHandlerRegistry = new InMemoryCommandHandlerRegistry(Collections.emptyList());
        testclient = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "testclient");
        commandHandlerRegistry.register(new CommandHandlerSubscription() {
            @Override
            public io.axoniq.axonserver.commandprocessing.spi.CommandHandler commandHandler() {
                return new io.axoniq.axonserver.commandprocessing.spi.CommandHandler() {
                    private final String id = UUID.randomUUID().toString();

                    @Override
                    public String id() {
                        return id;
                    }

                    @Override
                    public String description() {
                        return null;
                    }

                    @Override
                    public String commandName() {
                        return "Sample";
                    }

                    @Override
                    public String context() {
                        return "default";
                    }

                    @Override
                    public Metadata metadata() {
                        return new Metadata() {
                            @Override
                            public Iterable<String> metadataKeys() {
                                return null;
                            }

                            @Override
                            public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                                if (io.axoniq.axonserver.commandprocessing.spi.CommandHandler.COMPONENT_NAME.equals(
                                        metadataKey)) {
                                    return Optional.of((R) "testcomponent");
                                }
                                if (io.axoniq.axonserver.commandprocessing.spi.CommandHandler.CLIENT_ID.equals(
                                        metadataKey)) {
                                    return Optional.of((R) "Target");
                                }
                                return Optional.empty();
                            }
                        };
                    }
                };
            }

            @Override
            public Mono<CommandResult> dispatch(Command command) {
                return null;
            }
        }).block();
        commandMetricsRegistry = new CommandMetricsRegistry(new MeterFactory(new SimpleMeterRegistry(),
                                                                             new DefaultMetricCollector()));

        QueryRegistrationCache queryRegistrationCache = new QueryRegistrationCache(new RoundRobinQueryHandlerSelector());
        queryClient = new ClientStreamIdentification("context", "testclient");
        queryRegistrationCache.add(new QueryDefinition("context", "query"), "result",
                                   new QueryHandler<Object>(null,
                                                            queryClient, "testcomponent", "Target") {
                                       @Override
                                       public void dispatch(SubscriptionQueryRequest query) {

                                       }
                                   });
        principal = mock(Principal.class);
        when(principal.getName()).thenReturn("Testuser");
        queryMetricsRegistry = new QueryMetricsRegistry(new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector()));
        testSubject = new MetricsRestController(commandHandlerRegistry, commandMetricsRegistry,
                                                queryRegistrationCache, queryMetricsRegistry);
    }

    @Test
    public void getCommandMetrics() {
        List<CommandMetricsRegistry.CommandMetric> commands = testSubject.getCommandMetrics(principal).collectList()
                                                                         .block();
        assertEquals(1, commands.size());
        assertEquals("Target." + testclient.getContext(), commands.get(0).getClientId());
        assertEquals(0, commands.get(0).getCount());
        commandMetricsRegistry.add("Sample", "Source", "Target", testclient.getContext(), 1);
        commands = testSubject.getCommandMetrics(principal).collectList().block();
        assertEquals(1, commands.size());
        assertEquals("Target." + testclient.getContext(), commands.get(0).getClientId());
        assertEquals(1, commands.get(0).getCount());
    }

    @Test
    public void getQueryMetrics() {
        List<QueryMetricsRegistry.QueryMetric> queries = testSubject.getQueryMetrics(principal);
        assertEquals(1, queries.size());
        assertEquals("Target." + queryClient.getContext(), queries.get(0).getClientId());
        assertEquals(0, queries.get(0).getCount());

        queryMetricsRegistry.addHandlerResponseTime(new QueryDefinition("context", "query"),
                                                    "Source",
                                                    "Target",
                                                    queryClient.getContext(),
                                                    50);

        queries = testSubject.getQueryMetrics(principal);
        assertEquals(1, queries.size());
        assertEquals("Target." + queryClient.getContext(), queries.get(0).getClientId());
        assertEquals(1, queries.get(0).getCount());
    }
}
