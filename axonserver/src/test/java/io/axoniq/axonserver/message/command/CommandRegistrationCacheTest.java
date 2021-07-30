/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.component.command.FakeCommandHandler;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.message.ClientStreamIdentification;
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

    @Before
    public void setup() {
        registrationCache = new CommandRegistrationCache();

        registrationCache.add("command1",
                              new FakeCommandHandler(
                                      new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                     "client1"),
                                      "client1", "component"));
        registrationCache.add("command1",
                              new FakeCommandHandler(
                                      new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                     "client2"),
                                      "client2", "component"));
        registrationCache.add("command2",
                              new FakeCommandHandler(
                                      new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                     "client2"),
                                      "client2",
                                      "component"));
    }

    @Test
    public void removeCommandSubscription() {
        registrationCache.remove(new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client2"), "command1");
        Map<CommandHandler, Set<CommandRegistrationCache.RegistrationEntry>> registrations = registrationCache
                .getAll();
        assertTrue(registrations.containsKey(new FakeCommandHandler(
                new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                               "client2"),
                "client2",
                "component")));
        assertEquals(1,
                     registrations.get(new FakeCommandHandler(
                             new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                            "client2"), "client2",
                             "component")).size());
    }

    @Test
    public void singleDestinationShortcutTakesContextIntoAccount() {
        registrationCache.add("contextBCommand", new FakeCommandHandler(
                new ClientStreamIdentification("otherContext",
                                               "client1"),
                "client1", "component"));

        assertNotNull(registrationCache.getHandlerForCommand("otherContext",
                                                             Command.newBuilder().setName("contextBCommand").build(),
                                                             "irrelevant"));
        assertNull(registrationCache.getHandlerForCommand(Topology.DEFAULT_CONTEXT,
                                                          Command.newBuilder().setName("contextBCommand").build(),
                                                          "irrelevant"));
    }

    @Test
    public void removeLastCommandSubscription() {
        registrationCache.remove(new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client1"), "command1");
        assertFalse(registrationCache.getAll().containsKey(new FakeCommandHandler(
                new ClientStreamIdentification(
                        Topology.DEFAULT_CONTEXT,
                        "client1"), "client1",
                "component")));
    }

    @Test
    public void removeConnection() {
        registrationCache.remove(new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client2"));
        assertFalse(registrationCache.getAll().containsKey(new FakeCommandHandler(
                new ClientStreamIdentification(
                        Topology.DEFAULT_CONTEXT,
                        "client2"), "client2",
                "component")));
    }

    @Test
    public void add() {
        registrationCache.add("command2",
                              new FakeCommandHandler(
                                      new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                     "client1"), "client1",
                                      "component"));
        assertEquals(2,
                     registrationCache.getAll().get(new FakeCommandHandler(
                             new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                            "client1"),
                             "client1",
                             "component")).size());
    }

    @Test
    public void get() {
        assertNotNull(registrationCache.getHandlerForCommand(Topology.DEFAULT_CONTEXT,
                                                             Command.newBuilder().setName("command1").build(),
                                                             "command1"));
    }

    @Test
    public void getNotFound() {
        assertNull(registrationCache.getHandlerForCommand(Topology.DEFAULT_CONTEXT,
                                                          Command.newBuilder().setName("command3").build(),
                                                          "command1"));
    }

    @Test
    public void findByExistingClient() {
        assertNotNull(registrationCache.findByClientAndCommand(new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                              "client2"), "command1"));
    }

    @Test
    public void findByNonExistingClient() {
        assertNull(registrationCache.findByClientAndCommand(new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                           "client9"), "command1"));
    }
}
