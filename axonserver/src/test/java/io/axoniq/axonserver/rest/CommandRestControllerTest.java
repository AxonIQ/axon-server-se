/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.commandprocesing.imp.CommandSubscriptionCache;
import io.axoniq.axonserver.commandprocesing.imp.DefaultCommandRequestProcessor;
import io.axoniq.axonserver.commandprocesing.imp.InMemoryCommandHandlerRegistry;
import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.component.command.ComponentCommand;
import io.axoniq.axonserver.serializer.GsonMedia;
import io.axoniq.axonserver.topology.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandRestControllerTest {
    private CommandRestController testSubject;

    @Before
    public void setUp() {
        DefaultCommandRequestProcessor commandRequestProcessor = new DefaultCommandRequestProcessor(new InMemoryCommandHandlerRegistry());
        CommandSubscriptionCache commandSubscriptionCache = new CommandSubscriptionCache(commandRequestProcessor);
        Map<String, Serializable> metaDataMap = new HashMap<>();
        metaDataMap.put(CommandHandler.COMPONENT_NAME, "component");
        metaDataMap.put(CommandHandler.CLIENT_ID, "client");

        commandRequestProcessor.register(new CommandHandlerSubscription() {
            @Override
            public CommandHandler commandHandler() {
                return new CommandHandler() {
                    @Override
                    public String id() {
                        return "id";
                    }

                    @Override
                    public String description() {
                        return null;
                    }

                    @Override
                    public String commandName() {
                        return "DoIt";
                    }

                    @Override
                    public String context() {
                        return Topology.DEFAULT_CONTEXT;
                    }

                    @Override
                    public Metadata metadata() {
                        return new Metadata() {
                            @Override
                            public Iterable<String> metadataKeys() {
                                return metaDataMap.keySet();
                            }

                            @Override
                            public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                                return (Optional<R>) Optional.ofNullable(metaDataMap.get(metadataKey));
                            }
                        };
                    }
                };
            }

            @Override
            public Mono<CommandResult> dispatch(Command command) {
                return Mono.error(new RuntimeException("Not implemented"));
            }
        }).block();
        testSubject = new CommandRestController(commandRequestProcessor,
                                                commandSubscriptionCache);
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
