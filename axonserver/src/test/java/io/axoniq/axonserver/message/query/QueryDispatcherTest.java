/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;


import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.axoniq.axonserver.util.FailingStreamObserver;
import io.micrometer.core.instrument.Metrics;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryDispatcherTest {

    private QueryDispatcher testSubject;
    private final MeterFactory meterFactory = new MeterFactory(Metrics.globalRegistry, new DefaultMetricCollector());
    private final QueryMetricsRegistry queryMetricsRegistry = new QueryMetricsRegistry(meterFactory);
    private final QueryCache queryCache = new QueryCache(1000);

    @Mock
    private QueryRegistrationCache registrationCache;


    @Before
    public void setup() {
        testSubject = new QueryDispatcher(registrationCache,
                                              queryCache,
                                              queryMetricsRegistry,
                                              meterFactory,
                                              10_000);
    }

    @Test
    public void queryResponse() {
        AtomicInteger dispatchCalled = new AtomicInteger(0);
        AtomicBoolean doneCalled = new AtomicBoolean(false);
        queryCache.put("1234",new QueryInformation("1234",
                                                                     "Source",
                                                                     new QueryDefinition("c", "q"),
                                                                     Collections.singleton("client"),
                                                                     2,
                                                                     r -> dispatchCalled.incrementAndGet(),
                                                    (client) -> doneCalled.set(true)));
        testSubject.handleResponse(QueryResponse.newBuilder()
                                                    .setMessageIdentifier("12345")
                                                    .setRequestIdentifier("1234")
                                                    .build(), "client", "clientId", false);

        assertEquals(1, dispatchCalled.get());
        assertFalse(doneCalled.get());

        testSubject.handleResponse(QueryResponse.newBuilder()
                                                    .setMessageIdentifier("1234")
                                                    .setRequestIdentifier("1234")
                                                    .build(), "client", "clientId", false);

        assertEquals(2, dispatchCalled.get());
        assertTrue(doneCalled.get());
    }


    @Test
    public void queryNotFound() {
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();
        CountingStreamObserver<QueryResponse> responseObserver = new CountingStreamObserver<>();
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          responseObserver::onNext,
                          client -> responseObserver.onCompleted());
        assertTrue(responseObserver.completed);
        assertEquals(1, responseObserver.count);
        assertNotEquals("", responseObserver.responseList.get(0).getErrorCode());
    }

    @Test
    public void queryQueueFull() {
        testSubject = new QueryDispatcher(registrationCache, queryCache, queryMetricsRegistry, meterFactory, 0);
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .setClientId("sampleClient")
                                           .build();
        CountingStreamObserver<QueryResponse> responseObserver = new CountingStreamObserver<>();
        Set<QueryHandler> handlers = new HashSet<>();

        CountingStreamObserver<QueryProviderInbound> dispatchStreamObserver = new CountingStreamObserver<>();
        handlers.add(new DirectQueryHandler(dispatchStreamObserver,
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client"),
                                            "componentName",
                                            "client"));
        when(registrationCache.find(any(), any())).thenReturn(handlers);
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          responseObserver::onNext,
                          client -> responseObserver.onCompleted());
        assertTrue(responseObserver.completed);
        assertTrue(queryCache.isEmpty());
        assertEquals(1, responseObserver.count);
        assertNotEquals("", responseObserver.responseList.get(0).getErrorCode());
    }

    @Test
    public void queryFound() {
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();
        CountingStreamObserver<QueryResponse> responseObserver = new CountingStreamObserver<>();
        Set<QueryHandler> handlers = new HashSet<>();

        CountingStreamObserver<QueryProviderInbound> dispatchStreamObserver = new CountingStreamObserver<>();

        handlers.add(new DirectQueryHandler(dispatchStreamObserver,
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                           "client"),
                                            "componentName", "client"));
        when(registrationCache.find(any(), any())).thenReturn(handlers);
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                              responseObserver::onNext,
                              client -> responseObserver.onCompleted());
        assertEquals(0, responseObserver.count);
//        verify(queryCache, times(1)).put(any(), any());
    }

    //@Test
    public void queryError() {
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();
        CountingStreamObserver<QueryResponse> responseObserver = new CountingStreamObserver<>();
        Set<QueryHandler> handlers = new HashSet<>();

        handlers.add(new DirectQueryHandler(new FailingStreamObserver<>(),
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                           "client"),
                                            "componentName", "client"));
        when(registrationCache.find(any(), any())).thenReturn(handlers);
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request), responseObserver::onNext,
                              client -> responseObserver.onCompleted());
        assertEquals(1, responseObserver.count);
//        verify(queryCache, times(1)).put(any(), any());
    }

    @Test
    public void dispatchProxied() {
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();
        CountingStreamObserver<QueryProviderInbound> countingStreamObserver = new CountingStreamObserver<>();
        SerializedQuery forwardedQuery = new SerializedQuery(Topology.DEFAULT_CONTEXT, "client", request);

        QueryHandler handler = new DirectQueryHandler(countingStreamObserver,
                                                      new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                     "client"),
                                                      "componentName", "client");
        when(registrationCache.find(any(), anyObject(), anyObject())).thenReturn(handler);
        testSubject.dispatchProxied(forwardedQuery, r -> {
        }, s -> {
        });
        //assertEquals(1, countingStreamObserver.count);
//        verify(queryCache, times(1)).put(any(), any());
    }

    @Test
    public void dispatchProxiedNotFound() {
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();

        AtomicInteger callbackCount = new AtomicInteger(0);

        SerializedQuery forwardedQuery = new SerializedQuery(Topology.DEFAULT_CONTEXT, "client", request);
        testSubject.dispatchProxied(forwardedQuery, r -> callbackCount.incrementAndGet(), s -> {
        });
        assertEquals(1, callbackCount.get());
//        verify(queryCache, times(0)).put(any(), any());
    }

    //@Test
    public void dispatchProxiedWithError() throws Exception {
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();
        SerializedQuery forwardedQuery = new SerializedQuery(Topology.DEFAULT_CONTEXT, "client", request);
        AtomicInteger dispatchCount = new AtomicInteger(0);
        QueryHandler<?> handler = new DirectQueryHandler(new FailingStreamObserver<>(),
                                                         new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                        "client"),
                                                         "componentName", "client");
        when(registrationCache.find(any(), anyObject(), anyObject())).thenReturn(handler);
        testSubject.dispatchProxied(forwardedQuery, r -> dispatchCount.incrementAndGet(), s -> {
        });
        testSubject.getQueryQueue().take("client");
//        verify(queryCache, times(0)).put(any(), any());
    }

}
