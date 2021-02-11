/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;


import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.interceptor.NoOpQueryInterceptors;
import io.axoniq.axonserver.interceptor.QueryInterceptors;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.FailingStreamObserver;
import io.micrometer.core.instrument.Metrics;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryDispatcherTest {

    private final MeterFactory meterFactory = new MeterFactory(Metrics.globalRegistry, new DefaultMetricCollector());
    private final QueryMetricsRegistry queryMetricsRegistry = new QueryMetricsRegistry(meterFactory);
    private final QueryCache queryCache = new QueryCache(1000, 100);
    private QueryDispatcher testSubject;

    @Mock
    private QueryRegistrationCache registrationCache;

    @Before
    public void setup() {
        testSubject = new QueryDispatcher(registrationCache,
                                          queryCache,
                                          queryMetricsRegistry,
                                          new NoOpQueryInterceptors(),
                                          meterFactory,
                                          10_000);
    }

    @Test
    public void queryResponse() {
        AtomicInteger dispatchCalled = new AtomicInteger(0);
        AtomicBoolean doneCalled = new AtomicBoolean(false);
        queryCache.put("1234", new QueryInformation("1234",
                                                    "Source",
                                                    new QueryDefinition("c",
                                                                        "q"),

                                                    Collections.singleton("client"),
                                                    2,
                                                    r -> dispatchCalled.incrementAndGet(),
                                                    (client) -> doneCalled.set(true)));
        testSubject.handleResponse(QueryResponse.newBuilder()
                                                .setMessageIdentifier("12345")
                                                .setRequestIdentifier("1234")
                                                .build(), "client", "clientId",false);
        assertEquals(1, dispatchCalled.get());
        assertFalse(doneCalled.get());

        testSubject.handleResponse(QueryResponse.newBuilder()
                                                .setMessageIdentifier("1234")
                                                .setRequestIdentifier("1234")
                                                .build(), "client", "clientId",false);
        assertEquals(2, dispatchCalled.get());
        assertTrue(doneCalled.get());
    }


    @Test
    public void queryNotFound() {
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();
        FakeStreamObserver<QueryResponse> responseObserver = new FakeStreamObserver<>();
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL, responseObserver::onNext,
                          client -> responseObserver.onCompleted());
        assertEquals(1, responseObserver.completedCount());
        assertEquals(1, responseObserver.completedCount());
        assertNotEquals("", responseObserver.values().get(0).getErrorCode());
    }

    @Test
    public void queryQueueFull() {
        testSubject = new QueryDispatcher(registrationCache,
                                          queryCache,
                                          queryMetricsRegistry,
                                          new NoOpQueryInterceptors(),
                                          meterFactory,
                                          0);
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .setClientId("sampleClient")
                                           .build();
        FakeStreamObserver<QueryResponse> responseObserver = new FakeStreamObserver<>();
        Set<QueryHandler> handlers = new HashSet<>();

        FakeStreamObserver<QueryProviderInbound> dispatchStreamObserver = new FakeStreamObserver<>();
        handlers.add(new DirectQueryHandler(dispatchStreamObserver,
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client"),
                                            "componentName", "client"));
        when(registrationCache.find(any(), any())).thenReturn(handlers);
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL, responseObserver::onNext,
                          client -> responseObserver.onCompleted());
        assertEquals(1, responseObserver.completedCount());
        assertTrue(queryCache.isEmpty());
        assertEquals(1, responseObserver.values().size());
        assertNotEquals("", responseObserver.values().get(0).getErrorCode());
    }

    @Test
    public void queryFound() {
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();
        FakeStreamObserver<QueryResponse> responseObserver = new FakeStreamObserver<>();
        Set<QueryHandler> handlers = new HashSet<>();

        FakeStreamObserver<QueryProviderInbound> dispatchStreamObserver = new FakeStreamObserver<>();

        handlers.add(new DirectQueryHandler(dispatchStreamObserver,
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client"),
                                            "componentName", "client"));
        when(registrationCache.find(any(), any())).thenReturn(handlers);
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL, responseObserver::onNext,
                          client -> responseObserver.onCompleted());
        assertEquals(0, responseObserver.values().size());
    }

    @Test
    public void queryRequestRejected() throws ExecutionException, InterruptedException {
        testSubject = new QueryDispatcher(registrationCache,
                                          queryCache,
                                          queryMetricsRegistry,
                                          new MyQueryInterceptors(),
                                          meterFactory,
                                          10_000);
        QueryRequest request = QueryRequest.newBuilder()
                                           .setMessageIdentifier("REJECT")
                                           .setQuery("test")
                                           .build();

        CompletableFuture<QueryResponse> futureResponse = new CompletableFuture<>();
        CompletableFuture<Boolean> futureCompleted = new CompletableFuture<>();
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                          futureResponse::complete,
                          client -> futureCompleted.complete(true));
        QueryResponse response = futureResponse.get();
        assertEquals(ErrorCode.QUERY_REJECTED_BY_INTERCEPTOR.getCode(), response.getErrorCode());
        assertTrue(futureCompleted.get());
    }

    @Test
    public void queryRequestInterceptorFailed() throws ExecutionException, InterruptedException, TimeoutException {
        testSubject = new QueryDispatcher(registrationCache,
                                          queryCache,
                                          queryMetricsRegistry,
                                          new MyQueryInterceptors(),
                                          meterFactory,
                                          10_000);
        QueryRequest request = QueryRequest.newBuilder()
                                           .setMessageIdentifier("FAIL")
                                           .setQuery("test")
                                           .build();

        CompletableFuture<QueryResponse> futureResponse = new CompletableFuture<>();
        CompletableFuture<Boolean> futureCompleted = new CompletableFuture<>();
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                          futureResponse::complete,
                          client -> futureCompleted.complete(true));
        QueryResponse response = futureResponse.get(1, TimeUnit.SECONDS);
        assertEquals(ErrorCode.EXCEPTION_IN_INTERCEPTOR.getCode(), response.getErrorCode());
        assertTrue(futureCompleted.get(1, TimeUnit.SECONDS));
    }

    @Test
    public void queryResponseInterceptorFailed() throws ExecutionException, InterruptedException {
        QueryCache myQueryCache = new QueryCache(10000, 10000);
        testSubject = new QueryDispatcher(registrationCache,
                                          myQueryCache,
                                          queryMetricsRegistry,
                                          new MyQueryInterceptors(),
                                          meterFactory,
                                          10_000);
        QueryRequest request = QueryRequest.newBuilder()
                                           .setMessageIdentifier("RESPOND")
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();

        CompletableFuture<QueryResponse> futureResponse = new CompletableFuture<>();
        CompletableFuture<Boolean> futureCompleted = new CompletableFuture<>();
        Set<QueryHandler> handlers = Collections.singleton(new QueryHandler<QueryProviderInbound>(null,
                                                                                                  new ClientStreamIdentification(
                                                                                                          Topology.DEFAULT_CONTEXT,
                                                                                                          "clientStreamId"),
                                                                                                  null,
                                                                                                  null) {
            @Override
            public void dispatch(SubscriptionQueryRequest query) {

            }
        });
        when(registrationCache.find(any(), any())).thenReturn(handlers);
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                          futureResponse::complete,
                          client -> futureCompleted.complete(true));

        testSubject.handleResponse(QueryResponse.newBuilder().setMessageIdentifier("FAIL")
                                                .setRequestIdentifier(request.getMessageIdentifier()).build(),
                                   "clientStreamId",
                                   "clientId",
                                   false);

        QueryResponse response = futureResponse.get();
        assertEquals(ErrorCode.EXCEPTION_IN_INTERCEPTOR.getCode(), response.getErrorCode());
        testSubject.handleComplete(request.getMessageIdentifier(), "clientStreamId", "clientId", false);

        assertTrue(futureCompleted.get());
    }

    @Test
    public void dispatchProxied() {
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();
        FakeStreamObserver<QueryProviderInbound> FakeStreamObserver = new FakeStreamObserver<>();
        SerializedQuery forwardedQuery = new SerializedQuery(Topology.DEFAULT_CONTEXT, "client", request);

        QueryHandler<?> handler = new DirectQueryHandler(FakeStreamObserver,
                                                         new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                        "client"),
                                                         "componentName", "client");
        when(registrationCache.find(any(), any(), any())).thenReturn(handler);
        testSubject.dispatchProxied(forwardedQuery, r -> {
        }, s -> {
        });
        assertEquals(1, testSubject.getQueryQueue().getSegments().get("client.default").size());
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
        when(registrationCache.find(any(), any(), any())).thenReturn(handler);
        testSubject.dispatchProxied(forwardedQuery, r -> dispatchCount.incrementAndGet(), s -> {
        });
        assertEquals(1, testSubject.getQueryQueue().getSegments().get("client").size());
    }


    private static class MyQueryInterceptors implements QueryInterceptors {

        @Override
        public SerializedQuery queryRequest(SerializedQuery serializedQuery,
                                            ExtensionUnitOfWork extensionUnitOfWork) {
            if (serializedQuery.getMessageIdentifier().equals("REJECT")) {
                throw new MessagingPlatformException(ErrorCode.QUERY_REJECTED_BY_INTERCEPTOR, "Rejected");
            }
            if (serializedQuery.getMessageIdentifier().equals("FAIL")) {
                throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR, "Failed");
            }
            return serializedQuery;
        }

        @Override
        public QueryResponse queryResponse(QueryResponse response,
                                           ExtensionUnitOfWork extensionUnitOfWork) {
            if (response.getMessageIdentifier().equals("FAIL")) {
                throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR, "Failed");
            }
            return response;
        }
    }
}
