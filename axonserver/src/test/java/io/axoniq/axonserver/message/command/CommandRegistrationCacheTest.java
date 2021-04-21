/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.ClientStreamIdentification;
import io.axoniq.axonserver.refactoring.configuration.topology.Topology;
import io.axoniq.axonserver.refactoring.messaging.command.CommandRegistrationCache;
import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommandProviderInbound;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandHandler;
import io.axoniq.axonserver.refactoring.transport.grpc.DirectCommandHandler;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class CommandRegistrationCacheTest {

    private CommandRegistrationCache registrationCache;
    private StreamObserver<SerializedCommandProviderInbound> streamObserver1;
    private StreamObserver<SerializedCommandProviderInbound> streamObserver2;

    @Before
    public void setup() {
        registrationCache = new CommandRegistrationCache();

        streamObserver1 = new FakeStreamObserver<>();
        streamObserver2 = new FakeStreamObserver<>();

        registrationCache.add(
                new DummyCommandHandler("command1",
                                        "client1", "component", Topology.DEFAULT_CONTEXT));
        registrationCache.add(
                new DummyCommandHandler("command1",
                                        "client2", "component", Topology.DEFAULT_CONTEXT));

        registrationCache.add(
                new DummyCommandHandler("command2",
                                        "client2", "component", Topology.DEFAULT_CONTEXT));
    }

    @Test
    public void removeCommandSubscription() {
        registrationCache.remove(new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client2"), "command1");
        Map<CommandHandler, Set<CommandRegistrationCache.RegistrationEntry>> registrations = registrationCache
                .getAll();
        assertTrue(registrations.containsKey(new DirectCommandHandler(null,
                                                                      new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                                     "client2"),
                                                                      "client2",
                                                                      "component")));
        assertEquals(1,
                     registrations.get(new DirectCommandHandler(null,
                                                                new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                               "client2"), "client2",
                                                                "component")).size());
    }

    @Test
    public void singleDestinationShortcutTakesContextIntoAccount() {
//        registrationCache.add("contextBCommand", new DirectCommandHandler(null,new ClientStreamIdentification("otherContext",
//                                                                                                                         "client1" ),
//                                                                          "client1", "component"));

//        assertNotNull(registrationCache.getHandlerForCommand("otherContext", Command.newBuilder().setName("contextBCommand").build(), "irrelevant"));
//        assertNull(registrationCache.getHandlerForCommand(Topology.DEFAULT_CONTEXT, Command.newBuilder().setName("contextBCommand").build(), "irrelevant"));
        fail();
    }

    @Test
    public void removeLastCommandSubscription() {
        registrationCache.remove(new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client1"), "command1");
//        assertFalse(registrationCache.getAll().containsKey(new DummyCommandHandler(null,
//                                                                                    new ClientStreamIdentification(
//                                                                                            Topology.DEFAULT_CONTEXT,
//                                                                                            "client1"), "client1",
//                                                                                    "component")));
        fail();
    }

    @Test
    public void removeConnection() {
        registrationCache.remove(new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client2"));
//        assertFalse(registrationCache.getAll().containsKey(new DirectCommandHandler(null,
//                                                                                    new ClientStreamIdentification(
//                                                                                            Topology.DEFAULT_CONTEXT,
//                                                                                            "client2"), "client2",
//                                                                                    "component")));
        fail();
    }

    @Test
    public void add() {
//        registrationCache.add("command2",
//                              new DirectCommandHandler(null,
//                                                       new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
//                                                                                      "client1"), "client1",
//                                                       "component"));
//        assertEquals(2,
//                     registrationCache.getAll().get(new DirectCommandHandler(null,
//                                                                             new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
//                                                                                                            "client1"),
//                                                                             "client1",
//                                                                             "component")).size());
        fail();
    }

    @Test
    public void get() {
//        assertNotNull(registrationCache.getHandlerForCommand(Topology.DEFAULT_CONTEXT,
//                                                             Command.newBuilder().setName("command1").build(),
//                                                             "command1"));
    }

    @Test
    public void getNotFound() {
//        assertNull(registrationCache.getHandlerForCommand(Topology.DEFAULT_CONTEXT,
//                                                          Command.newBuilder().setName("command3").build(),
//                                                          "command1"));
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
