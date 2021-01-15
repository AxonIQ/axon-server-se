/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.CommandMetricsRegistry;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.message.query.RoundRobinQueryHandlerSelector;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.security.Principal;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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
        CommandRegistrationCache commandRegistrationCache = new CommandRegistrationCache();
        testclient = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "testclient");
        commandRegistrationCache.add("Sample", new CommandHandler<Object>(null,
                                                                          testclient, "Target",
                                                                          "testcomponent") {
            @Override
            public void dispatch(SerializedCommand request) {

            }

            @Override
            public void confirm(String messageId) {

            }

            @Override
            public int compareTo(@NotNull CommandHandler o) {
                return 0;
            }
        });
        commandMetricsRegistry = new CommandMetricsRegistry(new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector()));

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
        testSubject = new MetricsRestController(commandRegistrationCache, commandMetricsRegistry,
                                                queryRegistrationCache, queryMetricsRegistry);
    }

    @Test
    public void getCommandMetrics() {
        List<CommandMetricsRegistry.CommandMetric> commands = testSubject.getCommandMetrics(principal);
        assertEquals(1, commands.size());
        assertEquals("Target." + testclient.getContext(), commands.get(0).getClientId());
        assertEquals(0, commands.get(0).getCount());
        commandMetricsRegistry.add("Sample", "Source", "Target", testclient.getContext(), 1);
        commands = testSubject.getCommandMetrics(principal);
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
