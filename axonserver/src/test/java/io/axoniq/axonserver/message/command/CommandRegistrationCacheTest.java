/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class CommandRegistrationCacheTest {

    private CommandRegistrationCache registrationCache;
    private FlowControlQueues<WrappedCommand> commandQueue;

    @Before
    public void setup() {
        registrationCache = new CommandRegistrationCache();

        commandQueue = new FlowControlQueues<>();
        registrationCache.add("command1",
                              new DirectCommandHandler(new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                                                "client1"),
                                                       "component",
                                                       commandQueue));
        registrationCache.add("command1",
                              new DirectCommandHandler(new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                                                "client2"),
                                                       "component",
                                                       commandQueue));
        registrationCache.add("command2",
                              new DirectCommandHandler(new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                                                "client2"),
                                                       "component",
                                                       commandQueue));
    }

    @Test
    public void removeCommandSubscription() {
        registrationCache.remove(new ClientIdentification(Topology.DEFAULT_CONTEXT, "client2"), "command1");
        Map<CommandHandler, Set<CommandRegistrationCache.RegistrationEntry>> registrations = registrationCache
                .getAll();
        assertTrue(registrations.containsKey(new DirectCommandHandler(new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                                                               "client2"),
                                                                      "component",
                                                                      commandQueue)));
        assertEquals(1,
                     registrations.get(new DirectCommandHandler(new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                                                         "client2"),
                                                                "component",
                                                                commandQueue)).size());
    }

    @Test
    public void removeLastCommandSubscription() {
        registrationCache.remove(new ClientIdentification(Topology.DEFAULT_CONTEXT, "client1"), "command1");
        assertFalse(registrationCache.getAll()
                                     .containsKey(new DirectCommandHandler(new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                                                                    "client1"),
                                                                           "component",
                                                                           commandQueue)));
    }

    @Test
    public void removeConnection() {
        registrationCache.remove(new ClientIdentification(Topology.DEFAULT_CONTEXT, "client2"));
        assertFalse(registrationCache.getAll()
                                     .containsKey(new DirectCommandHandler(new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                                                                    "client2"),
                                                                           "component",
                                                                           commandQueue)));
    }

    @Test
    public void add() {
        registrationCache.add("command2",
                              new DirectCommandHandler(new ClientIdentification(Topology.DEFAULT_CONTEXT, "client1"),
                                                       "component",
                                                       commandQueue));
        assertEquals(2,
                     registrationCache.getAll()
                                      .get(new DirectCommandHandler(new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                                                             "client1"),
                                                                    "component",
                                                                    commandQueue)).size());
    }

    @Test
    public void get() {
        assertNotNull(registrationCache.getHandlerForCommand(Topology.DEFAULT_CONTEXT, Command.newBuilder().setName("command1").build(),
                                                             "command1"));
    }

    @Test
    public void getNotFound() {
        assertNull(registrationCache.getHandlerForCommand(Topology.DEFAULT_CONTEXT, Command.newBuilder().setName("command3").build(),
                                                          "command1"));
    }

    @Test
    public void findByExistingClient() {
        assertNotNull(registrationCache.findByClientAndCommand(new ClientIdentification(Topology.DEFAULT_CONTEXT,"client2"), "command1"));
    }

    @Test
    public void findByNonExistingClient() {
        assertNull(registrationCache.findByClientAndCommand(new ClientIdentification(Topology.DEFAULT_CONTEXT,"client9"), "command1"));
    }

}
