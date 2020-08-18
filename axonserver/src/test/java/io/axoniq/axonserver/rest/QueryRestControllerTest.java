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
import io.axoniq.axonserver.component.query.Query;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.query.DirectQueryHandler;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.serializer.GsonMedia;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class QueryRestControllerTest {
    private QueryRestController testSubject;
    private QueryRegistrationCache registationCache;
    private QueryDispatcher queryDispatcher;

    @Before
    public void setUp() throws Exception {
        registationCache = new QueryRegistrationCache(null);
        queryDispatcher = mock(QueryDispatcher.class);
        QuerySubscription querySubscription = QuerySubscription.newBuilder()
                                                               .setQuery("Request")
                                                               .setComponentName("Component")
                                                               .setClientId("client").build();
        ClientStreamIdentification clientStreamIdentification =
                new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, querySubscription.getClientId());
        registationCache.add(new QueryDefinition(Topology.DEFAULT_CONTEXT, querySubscription.getQuery()),
                             "Response",
                             new DirectQueryHandler(new FakeStreamObserver<>(),
                                                    clientStreamIdentification,
                                                    querySubscription.getComponentName(),
                                                    querySubscription.getClientId()));

        testSubject = new QueryRestController(registationCache, queryDispatcher);
    }

    @Test
    public void get() throws Exception {
        List<QueryRestController.JsonQueryMapping> queries = testSubject.get();
        assertNotEquals("[]", new ObjectMapper().writeValueAsString(queries));
    }

    @Test
    public void getByComponent(){
        Iterator<Query> iterator = testSubject.getByComponent("Component", Topology.DEFAULT_CONTEXT).iterator();
        assertTrue(iterator.hasNext());
        GsonMedia media = new GsonMedia();
        iterator.next().printOn(media);
        assertEquals("{\"name\":\"Request\",\"responseTypes\":[\"Response\"]}", media.toString());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void getByNotExistingComponent(){
        Iterator iterator = testSubject.getByComponent("otherComponent", Topology.DEFAULT_CONTEXT).iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void getByNotExistingContext(){
        Iterator iterator = testSubject.getByComponent("Component", "Dummy").iterator();
        assertFalse(iterator.hasNext());
    }
}
