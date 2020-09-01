/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.component.instance.FakeClient;
import io.axoniq.axonserver.grpc.DefaultClientIdRegistry;
import org.junit.*;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.topology.Topology.DEFAULT_CONTEXT;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 27/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ClientApplicationRestControllerTest {

    @Test
    public void getComponentInstances() {
        Clients clients = () -> asList( (Client) new FakeClient("clientA",DEFAULT_CONTEXT, true),
                                        new FakeClient("clientB",DEFAULT_CONTEXT, false),
                                        new FakeClient("clientC",DEFAULT_CONTEXT, false)).iterator();

        ClientApplicationRestController controller = new ClientApplicationRestController(clients,
                                                                                         new DefaultClientIdRegistry());
        Iterator iterator = controller.getComponentInstances("test", DEFAULT_CONTEXT, null).iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void listClients() {
        Client clientA = new FakeClient("clientA",DEFAULT_CONTEXT, true);
        Client clientB = new FakeClient("clientB",DEFAULT_CONTEXT, true);
        Client clientC = new FakeClient("clientC",DEFAULT_CONTEXT, true);

        Clients clients = () -> asList((Client) clientA, clientB, clientC).iterator();

        ClientApplicationRestController controller = new ClientApplicationRestController(clients,
                                                                                         new DefaultClientIdRegistry());
        List<Client> clientList = controller.listClients(null).collect(Collectors.toList());
        assertEquals(3, clientList.size());
        assertTrue(clientList.contains(clientA));
        assertTrue(clientList.contains(clientB));
        assertTrue(clientList.contains(clientC));
    }
}