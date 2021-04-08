/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.client.command;

import com.google.common.collect.ImmutableSet;
import io.axoniq.axonserver.ClientStreamIdentification;
import io.axoniq.axonserver.refactoring.configuration.topology.Topology;
import io.axoniq.axonserver.refactoring.messaging.command.CommandHandler;
import io.axoniq.axonserver.refactoring.messaging.command.CommandRegistrationCache;
import io.axoniq.axonserver.refactoring.transport.grpc.DirectCommandHandler;
import io.axoniq.axonserver.refactoring.transport.rest.serializer.GsonMedia;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public class DefaultCommandTest {

    private DefaultCommand defaultCommand;

    @Before
    public void setUp() throws Exception {
        ImmutableSet<CommandHandler> commandHandlers = ImmutableSet.of(new DirectCommandHandler(null,
                                                                                                new ClientStreamIdentification(
                                                                                                        Topology.DEFAULT_CONTEXT,
                                                                                                        "client"),
                                                                                                "client",
                                                                                                "componentA"));
        defaultCommand = new DefaultCommand(new CommandRegistrationCache.RegistrationEntry(Topology.DEFAULT_CONTEXT,
                                                                                           "commandName"),
                                            commandHandlers);
    }

    @Test
    public void belongsToComponent() {
        assertTrue(defaultCommand.belongsToComponent("componentA"));
    }

    @Test
    public void notBelongsToComponent() {
        assertFalse(defaultCommand.belongsToComponent("componentB"));
    }

    @Test
    public void printOn() {
        GsonMedia gsonMedia = new GsonMedia();
        defaultCommand.printOn(gsonMedia);
        assertEquals("{\"name\":\"commandName\"}", gsonMedia.toString());
    }
}