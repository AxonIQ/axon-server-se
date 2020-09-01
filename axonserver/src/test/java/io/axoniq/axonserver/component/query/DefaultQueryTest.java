/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.query;

import com.google.common.collect.ImmutableSet;
import io.axoniq.axonserver.message.query.DirectQueryHandler;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.serializer.GsonMedia;
import org.junit.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public class DefaultQueryTest {

    private DefaultQuery query;

    @Before
    public void setUp() throws Exception {
        QueryDefinition queryDefinition = new QueryDefinition("context", "queryName");
        Map<String, Set<QueryHandler<?>>> handlers = new HashMap<>();
        handlers.put("componentA", ImmutableSet.of(new DirectQueryHandler(null, null, null, null),
                                                   new DirectQueryHandler(null, null, null, null)));
        handlers.put("componentB", ImmutableSet.of());
        handlers.put("componentC", null);

        query = new DefaultQuery(queryDefinition, handlers, Collections.singleton(String.class.getName()));
    }

    @Test
    public void belongsToComponent() {
        assertTrue(query.belongsToComponent("componentA"));
    }

    @Test
    public void notBelongsToMissingComponent() {
        assertFalse(query.belongsToComponent("componentD"));
    }

    @Test
    public void notBelongsToComponentWithoutHandlers() {
        assertFalse(query.belongsToComponent("componentB"));
        assertFalse(query.belongsToComponent("componentC"));
    }

    @Test
    public void printOn() {
        GsonMedia gsonMedia = new GsonMedia();
        query.printOn(gsonMedia);
        assertEquals("{\"name\":\"queryName\",\"responseTypes\":[\"java.lang.String\"]}", gsonMedia.toString());
    }
}