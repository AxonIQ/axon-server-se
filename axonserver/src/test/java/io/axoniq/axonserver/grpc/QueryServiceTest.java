/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeQuery;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryProviderOutbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.interceptor.NoOpSubscriptionQueryInterceptors;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.WrappedQuery;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.DefaultTopology;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.mockito.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class QueryServiceTest {
    private QueryService testSubject;
    private QueryDispatcher queryDispatcher;
    private FlowControlQueues<WrappedQuery> queryQueue;
    private ApplicationEventPublisher eventPublisher;
    private String clientId = "name";

    @Before
    public void setUp()  {
        queryDispatcher = mock(QueryDispatcher.class);
        queryQueue = new FlowControlQueues<>();
        eventPublisher = mock(ApplicationEventPublisher.class);
        when(queryDispatcher.getQueryQueue()).thenReturn(queryQueue);
        MessagingPlatformConfiguration configuration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        Topology topology = new DefaultTopology(configuration);
        testSubject = new QueryService(topology,
                                       queryDispatcher,
                                       () -> Topology.DEFAULT_CONTEXT,
                                       () -> GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                       new DefaultClientIdRegistry(),
                                       new NoOpSubscriptionQueryInterceptors(),
                                       eventPublisher,
                                       new DefaultInstructionAckSource<>(ack -> QueryProviderInbound.newBuilder()
                                                                                                    .setAck(ack)
                                                                                                    .build()));
    }

    @Test
    public void flowControl() throws Exception {
        FakeStreamObserver<QueryProviderInbound> FakeStreamObserver = new FakeStreamObserver<>();
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(FakeStreamObserver);
        requestStream.onNext(QueryProviderOutbound.newBuilder().setFlowControl(FlowControl.newBuilder().setPermits(2)
                                                                                          .setClientId("name").build())
                                                  .build());
        Thread.sleep(250);
        assertEquals(1, queryQueue.getSegments().size());
        String key = queryQueue.getSegments().entrySet().iterator().next().getKey();
        String clientStreamId = key.substring(0, key.lastIndexOf("."));
        ClientStreamIdentification clientStreamIdentification =
                new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, clientStreamId);
        queryQueue.put(clientStreamIdentification.toString(), new WrappedQuery(
                clientStreamIdentification,
                "name",
                new SerializedQuery(Topology.DEFAULT_CONTEXT, "name",
                                    QueryRequest.newBuilder()
                                                .addProcessingInstructions(ProcessingInstructionHelper.timeout(10000))
                                                .build()), System.currentTimeMillis() + 2000));
        Thread.sleep(150);
        assertEquals(1, FakeStreamObserver.values().size());
        queryQueue.put(clientStreamIdentification.toString(), new WrappedQuery(
                clientStreamIdentification,
                "name",new SerializedQuery(Topology.DEFAULT_CONTEXT, "name", QueryRequest.newBuilder().build()),
                System.currentTimeMillis() - 2000));
        Thread.sleep(150);
        assertEquals(1, FakeStreamObserver.values().size());
        verify(queryDispatcher).removeFromCache(any(), any());
    }

    @Test
    public void subscribe() {
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(QueryProviderOutbound.newBuilder()
                                                  .setSubscribe(QuerySubscription.newBuilder().setClientId("name")
                                                                                 .setComponentName("component")
                                                                                 .setQuery("query"))
                                                  .build());
        verify(eventPublisher).publishEvent(isA(SubscribeQuery.class));
    }

    @Test
    public void unsupportedQueryInstruction() {
        FakeStreamObserver<QueryProviderInbound> responseStream = new FakeStreamObserver<>();
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(responseStream);

        String instructionId = "instructionId";
        requestStream.onNext(QueryProviderOutbound.newBuilder()
                                                  .setInstructionId(instructionId)
                                                  .build());

        InstructionAck ack = responseStream.values().get(responseStream.values().size() - 1)
                                           .getAck();
        assertEquals(instructionId, ack.getInstructionId());
        assertTrue(ack.hasError());
        assertEquals(ErrorCode.UNSUPPORTED_INSTRUCTION.getCode(), ack.getError().getErrorCode());
    }

    @Test
    public void unsupportedQueryInstructionWithoutInstructionId() {
        FakeStreamObserver<QueryProviderInbound> responseStream = new FakeStreamObserver<>();
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(responseStream);

        requestStream.onNext(QueryProviderOutbound.newBuilder().build());

        assertEquals(0, responseStream.values().size());
    }

    @Test
    public void unsubscribe()  {
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(QueryProviderOutbound.newBuilder()
                .setUnsubscribe(QuerySubscription.newBuilder().setClientId("name").setComponentName("component").setQuery("command"))
                .build());
        verify(eventPublisher, times(0)).publishEvent(isA(SubscriptionEvents.UnsubscribeQuery.class));
    }
    @Test
    public void unsubscribeAfterSubscribe() {
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(QueryProviderOutbound.newBuilder()
                .setSubscribe(QuerySubscription.newBuilder().setClientId("name").setComponentName("component").setQuery("command"))
                .build());
        requestStream.onNext(QueryProviderOutbound.newBuilder()
                .setUnsubscribe(QuerySubscription.newBuilder().setClientId("name").setComponentName("component").setQuery("command"))
                .build());
        verify(eventPublisher).publishEvent(isA(SubscriptionEvents.UnsubscribeQuery.class));
    }

    @Test
    public void cancelAfterSubscribe() {
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(QueryProviderOutbound.newBuilder()
                .setSubscribe(QuerySubscription.newBuilder().setClientId("name").setComponentName("component").setQuery("command"))
                .build());
        requestStream.onError(new RuntimeException("failed"));
    }

    @Test
    public void cancelBeforeSubscribe() {
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onError(new RuntimeException("failed"));
    }

    @Test
    public void close() {
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(QueryProviderOutbound.newBuilder().setFlowControl(FlowControl.newBuilder().setPermits(1).setClientId("name").build()).build());
        requestStream.onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void dispatch()  {
        doAnswer(invocationOnMock -> {
            Consumer<QueryResponse> callback = (Consumer<QueryResponse>) invocationOnMock.getArguments()[2];
            callback.accept(QueryResponse.newBuilder().build());
            return null;
        }).when(queryDispatcher).query(isA(SerializedQuery.class), any(), isA(Consumer.class), any());
        FakeStreamObserver<QueryResponse> responseObserver = new FakeStreamObserver<>();
        testSubject.query(QueryRequest.newBuilder().build(), responseObserver);
        assertEquals(1, responseObserver.values().size());
    }

    @Test
    public void queryHandlerDisconnected() {
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        clientId = "name";
        requestStream.onNext(QueryProviderOutbound.newBuilder()
                                                  .setSubscribe(QuerySubscription.newBuilder().setClientId(clientId)
                                                                                 .setComponentName("component")
                                                                                 .setQuery("command"))
                                                  .build());
        requestStream.onError(new RuntimeException("failed"));
        verify(eventPublisher).publishEvent(isA(TopologyEvents.QueryHandlerDisconnected.class));
    }

    @Test
    public void disconnectClientStream() {
        FakeStreamObserver<QueryProviderInbound> responseObserver = new FakeStreamObserver<>();
        StreamObserver<QueryProviderOutbound> requestStream = testSubject.openStream(responseObserver);
        requestStream.onNext(QueryProviderOutbound.newBuilder()
                                                  .setSubscribe(QuerySubscription.newBuilder()
                                                                                 .setClientId(clientId)
                                                                                 .setComponentName("component")
                                                                                 .setQuery("query"))
                                                  .build());
        requestStream.onNext(QueryProviderOutbound.newBuilder().setFlowControl(FlowControl.newBuilder()
                                                                                          .setPermits(100)
                                                                                          .setClientId(clientId)
                                                                                          .build()).build());
        ArgumentCaptor<SubscribeQuery> subscribe = ArgumentCaptor.forClass(SubscribeQuery.class);
        verify(eventPublisher).publishEvent(subscribe.capture());
        SubscribeQuery subscribeQuery = subscribe.getValue();
        ClientStreamIdentification streamIdentification = subscribeQuery.clientIdentification();
        testSubject.completeStreamForInactivity(clientId, streamIdentification);
        verify(eventPublisher).publishEvent(isA(TopologyEvents.QueryHandlerDisconnected.class));
        assertEquals(1, responseObserver.errors().size());
        assertTrue(responseObserver.errors().get(0).getMessage().contains("Query stream inactivity"));
    }
}
