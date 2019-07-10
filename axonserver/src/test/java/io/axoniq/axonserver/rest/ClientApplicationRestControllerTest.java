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
import org.junit.*;

import java.util.Iterator;
import java.util.List;

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

        ClientApplicationRestController controller = new ClientApplicationRestController(clients);
        Iterator iterator = controller.getComponentInstances("test", DEFAULT_CONTEXT).iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void listClients() {
        Clients clients = () -> asList( (Client) new FakeClient("clientA",DEFAULT_CONTEXT, true),
                                        new FakeClient("clientB",DEFAULT_CONTEXT, false),
                                        new FakeClient("clientC",DEFAULT_CONTEXT, false)).iterator();

        ClientApplicationRestController controller = new ClientApplicationRestController(clients);
        List<Client> clientList = controller.listClients();
        assertEquals(3, clientList.size());
    }
}