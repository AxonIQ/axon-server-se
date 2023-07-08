/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.commandprocesing.imp.CommandDispatcher;
import io.axoniq.axonserver.commandprocesing.imp.DefaultCommandRequestProcessor;
import io.axoniq.axonserver.commandprocesing.imp.InMemoryCommandHandlerRegistry;
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.ResultPayload;
import io.axoniq.axonserver.component.tags.ClientTagsCache;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Stefan Dragisic
 */
public class CommandGrpcControllerTest {

    CommandGrpcController testSubject;
    ContextProvider contextProvider;
    AuthenticationProvider authenticationProvider;
    CommandRequestProcessor commandRequestProcessor;
    CommandDispatcher queuedCommandDispatcher;
    ClientTagsCache clientTagsCache;

    @Before
    public void setUp() throws Exception {
        contextProvider = mock(ContextProvider.class);
        authenticationProvider = mock(AuthenticationProvider.class);
        commandRequestProcessor = mock(CommandRequestProcessor.class);
        queuedCommandDispatcher = mock(CommandDispatcher.class);
        clientTagsCache = mock(ClientTagsCache.class);

        Authentication authentication = mock(Authentication.class);
        when(authentication.getName()).thenReturn("user");

        when(contextProvider.getContext()).thenReturn("default");
        when(authenticationProvider.get()).thenReturn(authentication);

        testSubject = new CommandGrpcController(contextProvider,
                authenticationProvider,commandRequestProcessor,
                queuedCommandDispatcher,
                clientTagsCache);
    }
    
    @Test
    public void testDispatch() {
        Command command = Command.newBuilder().build();
        CommandResult commandResult = mock(CommandResult.class);
        when(commandResult.commandId()).thenReturn("commandId");
        when(commandResult.id()).thenReturn("id");
        when(commandResult.payload()).thenReturn(mock(ResultPayload.class));

        when(commandRequestProcessor.dispatch(any())).thenReturn(Mono.just(commandResult));
        FakeStreamObserver<CommandResponse> responseStream = new FakeStreamObserver<>();
        testSubject.dispatch(command,responseStream);

        assertEquals(responseStream.values().size(),1);
        CommandResponse commandResponse = responseStream.values().stream().findFirst().get();
        assertEquals(commandResponse.getMessageIdentifier(),"id");
        assertEquals(commandResponse.getRequestIdentifier(),"commandId");
    }

    @Test
    public void testDispatchError() {
        Command command = Command.newBuilder().build();
        when(commandRequestProcessor.dispatch(any())).thenReturn(Mono.error(new RuntimeException("commandError")));
        FakeStreamObserver<CommandResponse> responseStream = new FakeStreamObserver<>();
        testSubject.dispatch(command,responseStream);

        assertEquals(responseStream.values().size(),1);
        CommandResponse commandResponse = responseStream.values().stream().findFirst().get();
        assertTrue(commandResponse.getErrorMessage().getMessage().contains("commandError"));
    }

    @Test
    public void testOpenSteamSubscribe() {
        FakeStreamObserver<CommandProviderInbound> responseStream = new FakeStreamObserver<>();
        when(commandRequestProcessor.register(any())).thenReturn(Mono.empty());
        testSubject.openStream(responseStream)
                .onNext(CommandProviderOutbound.newBuilder()
                        .setSubscribe(CommandSubscription.newBuilder()
                        .setCommand("test").build()).build());

        verify(commandRequestProcessor).register(any());
    }

    @Test
    public void testOpenSteamUnsubscribe() {
        FakeStreamObserver<CommandProviderInbound> responseStream = new FakeStreamObserver<>();
        when(commandRequestProcessor.register(any())).thenReturn(Mono.empty());
        when(commandRequestProcessor.unregister(any())).thenReturn(Mono.empty());

        StreamObserver<CommandProviderOutbound> openStream = testSubject.openStream(responseStream);

        openStream.onNext(CommandProviderOutbound.newBuilder()
                        .setSubscribe(CommandSubscription.newBuilder()
                                .setCommand("test").build()).build());

        openStream
                .onNext(CommandProviderOutbound.newBuilder()
                        .setUnsubscribe(CommandSubscription.newBuilder()
                                .setCommand("test").build()).build());

        openStream.onCompleted();

        verify(commandRequestProcessor).unregister(any());
    }

    @Test
    public void testOpenSteamFlowControl() {
        FakeStreamObserver<CommandProviderInbound> responseStream = new FakeStreamObserver<>();
        when(commandRequestProcessor.register(any())).thenReturn(Mono.empty());
        doNothing().when(queuedCommandDispatcher).request(anyString(),anyInt());

        StreamObserver<CommandProviderOutbound> openStream = testSubject.openStream(responseStream);

        openStream.onNext(CommandProviderOutbound.newBuilder()
                .setSubscribe(CommandSubscription.newBuilder()
                        .setCommand("test").build()).build());

        openStream
                .onNext(CommandProviderOutbound.newBuilder()
                        .setFlowControl(FlowControl.newBuilder()
                                .setClientId("")
                                .setPermits(10)
                                .build()).build());

        verify(queuedCommandDispatcher).request("",10);
    }

    @Test
    public void testOpenSteamCommandResponse() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        FakeStreamObserver<CommandProviderInbound> responseStream = new FakeStreamObserver<>();
        commandRequestProcessor = new DefaultCommandRequestProcessor(new InMemoryCommandHandlerRegistry());

        testSubject = new CommandGrpcController(contextProvider,
                authenticationProvider,commandRequestProcessor,
                queuedCommandDispatcher,
                clientTagsCache);

        StreamObserver<CommandProviderOutbound> openStream = testSubject.openStream(responseStream);

        openStream.onNext(CommandProviderOutbound.newBuilder()
                .setSubscribe(CommandSubscription.newBuilder()
                        .setCommand("cmdMID").build()).build());


        io.axoniq.axonserver.commandprocessing.spi.Command cmd = mock(io.axoniq.axonserver.commandprocessing.spi.Command.class);
        when(cmd.id()).thenReturn("cmdID");
        when(cmd.commandName()).thenReturn("cmdMID");
        when(cmd.context()).thenReturn("default");
        when(cmd.metadata()).thenReturn(new Metadata() {
            @Override
            public Iterable<String> metadataKeys() {
                return Collections.emptyList();
            }

            @Override
            public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                return Optional.empty();
            }
        });

        commandRequestProcessor.dispatch(cmd)
                .doOnSuccess(s->countDownLatch.countDown())
                .timeout(Duration.ofMillis(2000))
                .subscribe();

        Thread.sleep(1000);

        openStream.onNext(CommandProviderOutbound.newBuilder()
                .setCommandResponse(CommandResponse.newBuilder()
                        .setRequestIdentifier("cmdID")
                        .setMessageIdentifier("cmdMID")
                        .build()).build());

        countDownLatch.await();

        assertEquals(responseStream.values().size(),2);
    }
}