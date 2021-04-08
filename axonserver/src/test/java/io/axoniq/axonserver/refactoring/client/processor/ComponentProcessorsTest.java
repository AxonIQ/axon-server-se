/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.client.processor;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.refactoring.client.processor.listener.ClientProcessor;
import io.axoniq.axonserver.refactoring.client.processor.listener.ClientProcessors;
import io.axoniq.axonserver.refactoring.client.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.refactoring.configuration.topology.Topology;
import org.junit.*;

import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ComponentProcessorsTest {

    @Test
    public void testOne() {
        ClientProcessor clientProcessor = new FakeClientProcessor("clientId",
                                                                  true,
                                                                  EventProcessorInfo.getDefaultInstance());

        ClientProcessors clientProcessors = () -> asList(new FakeClientProcessor("clientId",
                                                                                 false,
                                                                                 EventProcessorInfo
                                                                                         .getDefaultInstance()),
                                                         clientProcessor,
                                                         new FakeClientProcessor("clientId",
                                                                                 false,
                                                                                 EventProcessorInfo
                                                                                         .getDefaultInstance()))
                .iterator();

        ComponentEventProcessors processors = new ComponentEventProcessors("component",
                                                                           Topology.DEFAULT_CONTEXT,
                                                                           clientProcessors);
        Iterator<EventProcessor> iterator = processors.iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }
}