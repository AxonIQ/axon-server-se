/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeCommand;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.command.WrappedCommand;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.DefaultTopology;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.mockito.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class CommandServiceTest {

    private final String clientId = "name";
    private CommandService testSubject;
    private FlowControlQueues<WrappedCommand> commandQueue;
    private ApplicationEventPublisher eventPublisher;
    private CommandDispatcher commandDispatcher;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        commandDispatcher = mock(CommandDispatcher.class);
        commandQueue = new FlowControlQueues<>();
        eventPublisher = mock(ApplicationEventPublisher.class);

        when(commandDispatcher.getCommandQueues()).thenReturn(commandQueue);
        //when(commandDispatcher.redispatch(any(WrappedCommand.class))).thenReturn("test");
        MessagingPlatformConfiguration configuration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        Topology topology = new DefaultTopology(configuration);
        testSubject = new CommandService(topology,
                                         commandDispatcher,
                                         () -> Topology.DEFAULT_CONTEXT,
                                         () -> GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                         new DefaultClientIdRegistry(),
                                         eventPublisher,
                                         new DefaultInstructionAckSource<>(ack -> new SerializedCommandProviderInbound(
                                                 CommandProviderInbound.newBuilder().setAck(ack).build())));
    }

    @Test
    public void flowControl() throws Exception {
        FakeStreamObserver<SerializedCommandProviderInbound> fakeStreamObserver = new FakeStreamObserver<>();
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(fakeStreamObserver);
        requestStream.onNext(CommandProviderOutbound.newBuilder().setFlowControl(FlowControl.newBuilder().setPermits(1)
                                                                                            .setClientId("name")
                                                                                            .build()).build());
        Thread.sleep(150);
        assertEquals(1, commandQueue.getSegments().size());

        String key = commandQueue.getSegments().entrySet().iterator().next().getKey();
        String clientStreamId = key.substring(0, key.lastIndexOf("."));

        ClientStreamIdentification clientIdentification = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                             clientStreamId);
        commandQueue.put(clientIdentification.toString(), new WrappedCommand(clientIdentification,
                                                                             clientIdentification.getClientStreamId(),new SerializedCommand(Command.newBuilder()
                                                                                                          .build())));
        Thread.sleep(50);
        assertEquals(1, fakeStreamObserver.values().size());
    }

    @Test
    public void subscribe() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                                                    .setSubscribe(CommandSubscription.newBuilder().setClientId("name")
                                                                                     .setComponentName("component")
                                                                                     .setCommand("command"))
                                                    .build());
        verify(eventPublisher).publishEvent(isA(SubscribeCommand.class));
    }

    @Test
    public void unsupportedCommandInstruction() {
        FakeStreamObserver<io.axoniq.axonserver.grpc.SerializedCommandProviderInbound> responseStream = new FakeStreamObserver<>();
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(responseStream);

        String instructionId = "instructionId";
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                                                    .setInstructionId(instructionId)
                                                    .build());
        InstructionAckOrBuilder result = responseStream.values().get(responseStream.values().size() - 1)
                                                       .getInstructionResult();

        assertEquals(instructionId, result.getInstructionId());
        assertTrue(result.hasError());
        assertEquals(ErrorCode.UNSUPPORTED_INSTRUCTION.getCode(), result.getError().getErrorCode());
    }

    @Test
    public void unsupportedCommandInstructionWithoutInstructionId() {
        FakeStreamObserver<io.axoniq.axonserver.grpc.SerializedCommandProviderInbound> responseStream = new FakeStreamObserver<>();
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(responseStream);

        requestStream.onNext(CommandProviderOutbound.newBuilder().build());

        assertEquals(0, responseStream.values().size());
    }

    @Test
    public void unsubscribe() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                .setUnsubscribe(CommandSubscription.newBuilder().setClientId("name").setComponentName("component").setCommand("command"))
                .build());
        verify(eventPublisher, times(0)).publishEvent(isA(SubscriptionEvents.UnsubscribeCommand.class));
    }
    @Test
    public void unsubscribeAfterSubscribe() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                .setSubscribe(CommandSubscription.newBuilder().setClientId("name").setComponentName("component").setCommand("command"))
                .build());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                .setUnsubscribe(CommandSubscription.newBuilder().setClientId("name").setComponentName("component").setCommand("command"))
                .build());
        verify(eventPublisher).publishEvent(isA(SubscriptionEvents.UnsubscribeCommand.class));
    }

    @Test
    public void cancelAfterSubscribe() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                .setSubscribe(CommandSubscription.newBuilder().setClientId("name").setComponentName("component").setCommand("command"))
                .build());
        requestStream.onError(new RuntimeException("failed"));
    }

    @Test
    public void cancelBeforeSubscribe() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onError(new RuntimeException("failed"));
    }

    @Test
    public void close() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder().setFlowControl(FlowControl.newBuilder().setPermits(1).setClientId("name").build()).build());
        requestStream.onCompleted();
    }

    @Test
    public void dispatch() {
        doAnswer(invocationOnMock -> {
            Consumer<SerializedCommandResponse> responseConsumer = (Consumer<SerializedCommandResponse>) invocationOnMock
                    .getArguments()[3];
            responseConsumer.accept(new SerializedCommandResponse(CommandResponse.newBuilder().build()));
            return null;
        }).when(commandDispatcher).dispatch(any(), any(), any(), any());
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        testSubject.dispatch(Command.newBuilder().build().toByteArray(), responseObserver);
        assertEquals(1, responseObserver.values().size());
    }

    @Test
    public void commandHandlerDisconnected() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new FakeStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                                                    .setSubscribe(CommandSubscription.newBuilder().setClientId("name")
                                                                                     .setComponentName("component")
                                                                                     .setCommand("command"))
                                                    .build());
        requestStream.onError(new RuntimeException("failed"));
        verify(eventPublisher).publishEvent(isA(TopologyEvents.CommandHandlerDisconnected.class));
    }

    @Test
    public void disconnectClientStream() {
        FakeStreamObserver<SerializedCommandProviderInbound> responseObserver = new FakeStreamObserver<>();
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(responseObserver);
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                                                    .setSubscribe(CommandSubscription.newBuilder()
                                                                                     .setClientId(clientId)
                                                                                     .setComponentName("component")
                                                                                     .setCommand("command"))
                                                    .build());
        requestStream.onNext(CommandProviderOutbound.newBuilder().setFlowControl(FlowControl.newBuilder()
                                                                                            .setPermits(100)
                                                                                            .setClientId(clientId)
                                                                                            .build()).build());
        ArgumentCaptor<SubscribeCommand> subscribe = ArgumentCaptor.forClass(SubscribeCommand.class);
        verify(eventPublisher).publishEvent(subscribe.capture());
        SubscribeCommand subscribeCommand = subscribe.getValue();
        ClientStreamIdentification streamIdentification = subscribeCommand.getHandler().getClientStreamIdentification();
        testSubject.completeStreamForInactivity(clientId, streamIdentification);
        verify(eventPublisher).publishEvent(isA(TopologyEvents.CommandHandlerDisconnected.class));
        assertEquals(1, responseObserver.errors().size());
        assertTrue(responseObserver.errors().get(0).getMessage().contains("Command stream inactivity"));
    }
}
