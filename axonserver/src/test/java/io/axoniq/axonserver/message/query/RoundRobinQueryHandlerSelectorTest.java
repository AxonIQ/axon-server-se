/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;

import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class RoundRobinQueryHandlerSelectorTest {
    private RoundRobinQueryHandlerSelector testSubject;

    @Before
    public void setUp() throws Exception {
        testSubject = new RoundRobinQueryHandlerSelector();
    }

    @Test
    public void select() {
        NavigableSet<ClientIdentification> clients = new TreeSet<>();
        clients.add(new ClientIdentification(Topology.DEFAULT_CONTEXT, "client1"));
        clients.add(new ClientIdentification(Topology.DEFAULT_CONTEXT, "client2"));
        ClientIdentification selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                                                           clients);
        assertEquals("client1", selected.getClient());
        selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client2", selected.getClient());
        selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client1", selected.getClient());
    }

    @Test
    public void selectWithoutLast() {
        NavigableSet<ClientIdentification> clients = new TreeSet<>();
        clients.add(new ClientIdentification(Topology.DEFAULT_CONTEXT, "client1"));
        ClientIdentification selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client1", selected.getClient());
        clients = new TreeSet<>();
        clients.add(new ClientIdentification(Topology.DEFAULT_CONTEXT, "client2"));
        selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client2", selected.getClient());
        selected = testSubject.select(new QueryDefinition("context", "request"), "component",
                clients);
        assertEquals("client2", selected.getClient());
    }

}
