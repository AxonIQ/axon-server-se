/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.component.command.ComponentCommand;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.command.DirectCommandHandler;
import io.axoniq.axonserver.serializer.GsonMedia;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandRestControllerTest {
    private CommandRestController testSubject;
    @Mock
    private CommandDispatcher commandDispatcher;

    @Before
    public void setUp() {
        CommandRegistrationCache commandRegistationCache = new CommandRegistrationCache();
        commandRegistationCache.add("DoIt",
                                    new DirectCommandHandler(new FakeStreamObserver<>(),
                                                             new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                      "client"),
                                                            "client",
                                                             "component"));
        testSubject = new CommandRestController(commandDispatcher, commandRegistationCache);
    }

    @Test
    public void get() throws Exception {
        List<CommandRestController.JsonClientMapping> commands = testSubject.get(null);
        ObjectMapper mapper = new ObjectMapper();
        assertNotEquals("[]", mapper.writeValueAsString(commands));
    }

    @Test
    public void getByComponent(){
        Iterator<ComponentCommand> iterator = testSubject.getByComponent("component", Topology.DEFAULT_CONTEXT, null).iterator();
        assertTrue(iterator.hasNext());
        GsonMedia gsonMedia = new GsonMedia();
        iterator.next().printOn(gsonMedia);
        assertEquals("{\"name\":\"DoIt\"}", gsonMedia.toString());
        assertFalse(iterator.hasNext());

    }

    @Test
    public void getByNotExistingComponent(){
        Iterator<ComponentCommand> iterator = testSubject.getByComponent("otherComponent", Topology.DEFAULT_CONTEXT, null).iterator();
        assertFalse(iterator.hasNext());
    }


}
