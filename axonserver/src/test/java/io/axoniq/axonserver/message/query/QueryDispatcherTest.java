/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.interceptor.NoOpQueryInterceptors;
import io.axoniq.axonserver.interceptor.QueryInterceptors;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.plugin.ExecutionContext;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
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
        queryCache.putIfAbsent("1234", new ActiveQuery("1234",
                                                       serializedQuery(),
                                                       r -> dispatchCalled.incrementAndGet(),
                                                       (client) -> doneCalled.set(true), mockedQueryHandler()));
        testSubject.handleResponse(QueryResponse.newBuilder()
                                                .setMessageIdentifier("12345")
                                                .setRequestIdentifier("1234")
                                                .build(), "client", "clientId");
        assertEquals(1, dispatchCalled.get());
        assertFalse(doneCalled.get());

        testSubject.handleResponse(QueryResponse.newBuilder()
                                                .setMessageIdentifier("1234")
                                                .setRequestIdentifier("1234")
                                                .build(), "client", "clientId");
        testSubject.handleComplete("1234", "client", "clientId", false);
        assertEquals(2, dispatchCalled.get());
        assertTrue(doneCalled.get());
    }

    @Test
    public void queryResponseScatterGather() {
        AtomicInteger dispatchCalled = new AtomicInteger(0);
        AtomicBoolean doneCalled = new AtomicBoolean(false);
        queryCache.putIfAbsent("1234", new ActiveQuery("1234",
                                                       serializedQuery(),
                                                       r -> dispatchCalled.incrementAndGet(),
                                                       (client) -> doneCalled.set(true), mockedQueryHandlers()));
        testSubject.handleResponse(QueryResponse.newBuilder()
                                                .setMessageIdentifier("12345")
                                                .setRequestIdentifier("1234")
                                                .build(), "client", "clientId1");
        assertEquals(1, dispatchCalled.get());
        assertFalse(doneCalled.get());
        testSubject.handleComplete("1234", "client1", "clientId1", false);
        assertFalse(doneCalled.get());

        testSubject.handleResponse(QueryResponse.newBuilder()
                                                .setMessageIdentifier("1234")
                                                .setRequestIdentifier("1234")
                                                .build(), "client2", "clientId2");
        testSubject.handleComplete("1234", "client2", "clientId2", false);
        assertEquals(2, dispatchCalled.get());
        assertTrue(doneCalled.get());
    }

    @Test
    public void queryNotFound() {
        String requestIdentifier = "1234";
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier(requestIdentifier)
                                           .build();
        FakeStreamObserver<QueryResponse> responseObserver = new FakeStreamObserver<>();
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL, responseObserver::onNext,
                          client -> responseObserver.onCompleted());
        assertEquals(1, responseObserver.values().size());
        assertEquals(1, responseObserver.completedCount());
        assertEquals(requestIdentifier, responseObserver.values().get(0).getRequestIdentifier());
        assertEquals(ErrorCode.NO_HANDLER_FOR_QUERY.getCode(), responseObserver.values().get(0).getErrorCode());
    }

    @Test
    public void queryQueueFull() {
        testSubject = new QueryDispatcher(registrationCache,
                                          queryCache,
                                          queryMetricsRegistry,
                                          new NoOpQueryInterceptors(),
                                          meterFactory,
                                          0);
        String requestId = "1234";
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier(requestId)
                                           .setClientId("sampleClient")
                                           .build();
        FakeStreamObserver<QueryResponse> responseObserver = new FakeStreamObserver<>();
        Set<QueryHandler<?>> handlers = new HashSet<>();

        FakeStreamObserver<QueryProviderInbound> dispatchStreamObserver = new FakeStreamObserver<>();
        handlers.add(new DirectQueryHandler(dispatchStreamObserver,
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client"),
                                            "componentName", "client"));
        when(registrationCache.find(any(String.class), any())).thenReturn(handlers);
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL, responseObserver::onNext,
                          client -> responseObserver.onCompleted());
        assertEquals(1, responseObserver.completedCount());
        assertTrue(queryCache.isEmpty());
        assertEquals(1, responseObserver.values().size());
        assertEquals(ErrorCode.TOO_MANY_REQUESTS.getCode(), responseObserver.values().get(0).getErrorCode());
        assertEquals(requestId, responseObserver.values().get(0).getRequestIdentifier());
    }

    @Test
    public void queryFound() {
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier("1234")
                                           .build();
        FakeStreamObserver<QueryResponse> responseObserver = new FakeStreamObserver<>();
        Set<QueryHandler<?>> handlers = new HashSet<>();

        FakeStreamObserver<QueryProviderInbound> dispatchStreamObserver = new FakeStreamObserver<>();

        handlers.add(new DirectQueryHandler(dispatchStreamObserver,
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client"),
                                            "componentName", "client"));
        when(registrationCache.find(any(String.class), any())).thenReturn(handlers);
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
        String requestId = "REJECT";
        QueryRequest request = QueryRequest.newBuilder()
                                           .setMessageIdentifier(requestId)
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
        assertEquals(requestId, response.getRequestIdentifier());
    }

    @Test
    public void queryRequestInterceptorFailed() throws ExecutionException, InterruptedException, TimeoutException {
        testSubject = new QueryDispatcher(registrationCache,
                                          queryCache,
                                          queryMetricsRegistry,
                                          new MyQueryInterceptors(),
                                          meterFactory,
                                          10_000);
        String requestId = "FAIL";
        QueryRequest request = QueryRequest.newBuilder()
                                           .setMessageIdentifier(requestId)
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
        assertEquals(requestId, response.getRequestIdentifier());
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
        Set<QueryHandler<?>> handlers = Collections.singleton(new QueryHandler<QueryProviderInbound>(null,
                                                                                                  new ClientStreamIdentification(
                                                                                                          Topology.DEFAULT_CONTEXT,
                                                                                                          "clientStreamId"),
                                                                                                  null,
                                                                                                  null) {
            @Override
            public void dispatch(SubscriptionQueryRequest query) {

            }
        });
        when(registrationCache.find(any(String.class), any())).thenReturn(handlers);
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                          futureResponse::complete,
                          client -> futureCompleted.complete(true));

        testSubject.handleResponse(QueryResponse.newBuilder()
                                                .setMessageIdentifier("FAIL")
                                                .setRequestIdentifier(request.getMessageIdentifier())
                                                .build(),
                                   "clientStreamId",
                                   "clientId");

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

    @Test
    public void clientDisconnectedError(){
        String requestId = "1234";
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier(requestId)
                                           .build();
        SerializedQuery forwardedQuery = new SerializedQuery(Topology.DEFAULT_CONTEXT, "client", request);
        when(registrationCache.find(any(), any(), any())).thenReturn(null);
        List<QueryResponse> responses = new LinkedList<>();
        AtomicReference<String> complete = new AtomicReference<>();
        testSubject.dispatchProxied(forwardedQuery, responses::add, complete::set);

        assertEquals(1, responses.size());
        assertEquals(requestId, responses.get(0).getRequestIdentifier());
        assertEquals(ErrorCode.CLIENT_DISCONNECTED.getCode(), responses.get(0).getErrorCode());
    }

//    @Test
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

    @Test
    public void queryCancellation() throws InterruptedException {
        String requestId = "1234";
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier(requestId)
                                           .build();

        Set<QueryHandler<?>> handlers = new HashSet<>();

        FakeStreamObserver<QueryProviderInbound> dispatchStreamObserver = new FakeStreamObserver<>();
        handlers.add(new DirectQueryHandler(dispatchStreamObserver,
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client"),
                                            "componentName", "client"));
        when(registrationCache.find(any(String.class), any())).thenReturn(handlers);

        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                          q -> { },
                          d -> { });

        testSubject.cancel(requestId);

        FlowControlQueues<QueryInstruction> queue = testSubject.getQueryQueue();
        QueryInstruction instruction = queue.take("client.default");
        assertNull(instruction);
    }

    @Test
    public void queryFlowControl() throws InterruptedException {
        String requestId = "1234";
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier(requestId)
                                           .build();

        Set<QueryHandler<?>> handlers = new HashSet<>();

        FakeStreamObserver<QueryProviderInbound> dispatchStreamObserver = new FakeStreamObserver<>();
        handlers.add(new DirectQueryHandler(dispatchStreamObserver,
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client"),
                                            "componentName", "client"));
        when(registrationCache.find(any(String.class), any())).thenReturn(handlers);

        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                          q -> { },
                          d -> { });

        testSubject.flowControl(requestId, 100);

        FlowControlQueues<QueryInstruction> queue = testSubject.getQueryQueue();
        QueryInstruction instruction = queue.take("client.default");
        assertTrue(instruction.query().isPresent());
        instruction = queue.take("client.default");
        assertTrue(instruction.flowControl().isPresent());
        assertEquals(requestId, instruction.requestId());
        assertEquals(100, instruction.flowControl().get().flowControl());
    }

    @Test
    public void queryDuplicated() throws ExecutionException, InterruptedException, TimeoutException {

        String requestId = "AAAAA";
        // futures for the original request, in normal operation, these should both complete
        CompletableFuture<QueryResponse> originalFutureResponse = new CompletableFuture<>();
        CompletableFuture<Boolean> originalFutureCompleted = new CompletableFuture<>();

        // set up handlers so AS can find one to dispatch to
        Set<QueryHandler<?>> handlers = new HashSet<>();
        FakeStreamObserver<QueryProviderInbound> dispatchStreamObserver = new FakeStreamObserver<>();
        handlers.add(new DirectQueryHandler(dispatchStreamObserver,
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client"),
                                            "componentName", "clientId"));
        when(registrationCache.find(any(String.class), any())).thenReturn(handlers);


        // this query might have been created earlier and already is in the cache
        queryCache.putIfAbsent(requestId, new ActiveQuery(requestId,
                                                          serializedQuery(),
                                                          originalFutureResponse::complete,
                                                          (client) -> originalFutureCompleted.complete(true),
                                                          mockedQueryHandler()));

        testSubject = new QueryDispatcher(registrationCache,
                                          queryCache,
                                          queryMetricsRegistry,
                                          new MyQueryInterceptors(),
                                          meterFactory,
                                          10_000);

        // this request has THE SAME requestId as the one already in the cache
        QueryRequest duplicatedRequest = QueryRequest.newBuilder()
                                           .setMessageIdentifier(requestId)
                                           .setQuery("test")
                                           .build();

        CompletableFuture<QueryResponse> futureResponse = new CompletableFuture<>();
        CompletableFuture<Boolean> futureCompleted = new CompletableFuture<>();
        testSubject.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, duplicatedRequest),
                          GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                          futureResponse::complete,
                          client -> futureCompleted.complete(true));

        testSubject.handleResponse(QueryResponse.newBuilder()
                                                .setMessageIdentifier(requestId)
                                                .setRequestIdentifier(duplicatedRequest.getMessageIdentifier())
                                                .build(),
                                   "client",
                                   "clientId");

        testSubject.handleComplete(requestId, "client", "clientId", false);

        // would not finish, if AS would override query in query cache
        QueryResponse originalResponse = originalFutureResponse.get(1, TimeUnit.SECONDS);
        assertTrue(originalFutureCompleted.get(1, TimeUnit.SECONDS));
        assertEquals(requestId, originalResponse.getRequestIdentifier());

        assertTrue(futureCompleted.get());
        QueryResponse response = futureResponse.get();
        assertEquals(response.getErrorCode(), ErrorCode.QUERY_DUPLICATED.getCode());
    }

    private static class MyQueryInterceptors implements QueryInterceptors {

        @Override
        public SerializedQuery queryRequest(SerializedQuery serializedQuery,
                                            ExecutionContext executionContext) {
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
                                           ExecutionContext executionContext) {
            if (response.getMessageIdentifier().equals("FAIL")) {
                throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR, "Failed");
            }
            return response;
        }
    }

    private Set<QueryHandler<?>> mockedQueryHandler() {
        QueryHandler<?> handler = mock(QueryHandler.class);
        when(handler.getClientStreamId()).thenReturn("client");
        return Collections.singleton(handler);
    }

    private Set<QueryHandler<?>> mockedQueryHandlers() {
        QueryHandler<?> handler1 = mock(QueryHandler.class);
        when(handler1.getClientStreamId()).thenReturn("client1");
        QueryHandler<?> handler2 = mock(QueryHandler.class);
        when(handler2.getClientStreamId()).thenReturn("client2");
        return new HashSet<>(asList(handler1, handler2));
    }
    // TODO
    //ErrorCode.TOO_MANY_REQUESTS
    //ErrorCode.OTHER

    private SerializedQuery serializedQuery() {
        return new SerializedQuery("c", "Source", QueryRequest.newBuilder()
                                                              .setQuery("q")
                                                              .build());
    }

    @Test
    public void cancelRemoveTheInstructionFromDestinationQueue() throws InterruptedException {
        QueryCache myQueryCache = new QueryCache(100, 100);
        QueryDispatcher queryDispatcher = new QueryDispatcher(registrationCache,
                                                              myQueryCache,
                                                              queryMetricsRegistry,
                                                              new MyQueryInterceptors(),
                                                              meterFactory,
                                                              10_000);
        String requestId = "1234";
        QueryRequest request = QueryRequest.newBuilder()
                                           .setQuery("test")
                                           .setMessageIdentifier(requestId)
                                           .build();
        Set<QueryHandler<?>> handlers = new HashSet<>();
        FakeStreamObserver<QueryProviderInbound> dispatchStreamObserver = new FakeStreamObserver<>();
        handlers.add(new DirectQueryHandler(dispatchStreamObserver,
                                            new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client"),
                                            "componentName", "client"));
        when(registrationCache.find(any(String.class), any())).thenReturn(handlers);
        AtomicReference<String> completion = new AtomicReference<>();
        queryDispatcher.query(new SerializedQuery(Topology.DEFAULT_CONTEXT, request),
                              GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                              r -> {},
                              completion::set);
        queryDispatcher.cancel(requestId);
        assertTrue(queryCache.isEmpty());
        assertNull(queryDispatcher.getQueryQueue().take("client.default"));
        assertNotNull(completion.get());
    }
}
