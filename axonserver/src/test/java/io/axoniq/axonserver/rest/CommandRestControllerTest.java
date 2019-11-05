/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.component.command.ComponentCommand;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.command.DirectCommandHandler;
import io.axoniq.axonserver.serializer.GsonMedia;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

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
        commandRegistationCache.add("DoIt", new DirectCommandHandler(new CountingStreamObserver<>(), new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                                     "client"), "component"));
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
