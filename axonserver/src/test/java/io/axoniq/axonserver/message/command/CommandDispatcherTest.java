/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.applicationevents.TopologyEvents.CommandHandlerDisconnected;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.interceptor.CommandInterceptors;
import io.axoniq.axonserver.interceptor.NoOpCommandInterceptors;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandDispatcherTest {

    private CommandDispatcher commandDispatcher;
    MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());
    private CommandMetricsRegistry metricsRegistry;
    @Mock
    private CommandCache commandCache;
    @Mock
    private CommandRegistrationCache registrations;

    @Before
    public void setup() {
        metricsRegistry = new CommandMetricsRegistry(meterFactory);
        commandDispatcher = new CommandDispatcher(registrations, commandCache, metricsRegistry, meterFactory,
                                                  new NoOpCommandInterceptors(), 10_000);
    }

    @Test
    public void unregisterCommandHandler()  {
        commandDispatcher.on(new CommandHandlerDisconnected(null, "clientId", "client", false));
    }

    @Test
    public void dispatch()  {
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        FakeStreamObserver<SerializedCommandProviderInbound> commandProviderInbound = new FakeStreamObserver<>();
        ClientStreamIdentification client = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client");
        DirectCommandHandler result = new DirectCommandHandler(commandProviderInbound,
                                                               client, "client", "component");
        when(registrations.getHandlerForCommand(eq(Topology.DEFAULT_CONTEXT), any(), any())).thenReturn(result);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT,
                                   GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                   new SerializedCommand(request),
                                   response -> {
                                       responseObserver.onNext(response);
                                       responseObserver.onCompleted();
                                   });
        assertEquals(1, commandDispatcher.getCommandQueues().getSegments().get(client.toString()).size());
        assertEquals(0, responseObserver.values().size());
        Mockito.verify(commandCache, times(1)).put(eq("12"), any());
    }

    @Test
    public void dispatchNotFound() {
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        when(registrations.getHandlerForCommand(any(), any(), any())).thenReturn(null);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT,
                                   GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                   new SerializedCommand(request),
                                   response -> {
                                       responseObserver.onNext(response);
                                       responseObserver.onCompleted();
                                   });
        assertEquals(1, responseObserver.values().size());
        assertNotEquals("", responseObserver.values().get(0).getErrorCode());
        Mockito.verify(commandCache, times(0)).put(eq("12"), any());
    }

    @Test
    public void dispatchQueueFull() {
        commandDispatcher = new CommandDispatcher(registrations,
                                                  commandCache,
                                                  metricsRegistry,
                                                  meterFactory,
                                                  new NoOpCommandInterceptors(),
                                                  0);
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        FakeStreamObserver<SerializedCommandProviderInbound> commandProviderInbound = new FakeStreamObserver<>();
        ClientStreamIdentification client = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client");
        DirectCommandHandler result = new DirectCommandHandler(commandProviderInbound,
                                                               client, "client", "component");
        when(registrations.getHandlerForCommand(any(), any(), any())).thenReturn(result);
        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT,
                                   GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                   new SerializedCommand(request),
                                   response -> {
                                       responseObserver.onNext(response);
                                       responseObserver.onCompleted();
                                   });
        assertEquals(1, responseObserver.values().size());
        assertNotEquals("", responseObserver.values().get(0).getErrorCode());
    }

    @Test
    public void dispatchUnknownContext() {
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        when(registrations.getHandlerForCommand(any(), any(), any())).thenReturn(null);

        commandDispatcher.dispatch("UnknownContext",
                                   GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                   new SerializedCommand(request),
                                   response -> {
                                       responseObserver.onNext(response);
                                       responseObserver.onCompleted();
                                   });
        assertEquals(1, responseObserver.values().size());
        assertEquals("AXONIQ-4000", responseObserver.values().get(0).getErrorCode());
        Mockito.verify(commandCache, times(0)).put(eq("12"), any());
    }

    @Test
    public void dispatchProxied() throws Exception {
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        Command request = Command.newBuilder()
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        ClientStreamIdentification clientIdentification = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client");
        FakeStreamObserver<SerializedCommandProviderInbound> commandProviderInbound = new FakeStreamObserver<>();
        DirectCommandHandler result = new DirectCommandHandler(commandProviderInbound,
                                                               clientIdentification,
                                                               "client",
                                                               "component");
        when(registrations.findByClientAndCommand(eq(clientIdentification), any())).thenReturn(result);

        commandDispatcher.dispatchProxied(Topology.DEFAULT_CONTEXT,
                                          new SerializedCommand(request.toByteArray(),
                                                                "client",
                                                                request.getMessageIdentifier()),
                                          responseObserver::onNext);
        assertEquals(1, commandDispatcher.getCommandQueues().getSegments().get(clientIdentification.toString()).size());
        assertEquals("12", commandDispatcher.getCommandQueues().take(clientIdentification.toString()).command()
                                            .getMessageIdentifier());
        assertEquals(0, responseObserver.values().size());
        Mockito.verify(commandCache, times(1)).put(eq("12"), any());
    }

    @Test
    public void dispatchProxiedClientNotFound()  {
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();

        commandDispatcher.dispatchProxied(Topology.DEFAULT_CONTEXT,
                                          new SerializedCommand(request),
                                          responseObserver::onNext);
        assertEquals(1, responseObserver.values().size());
        Mockito.verify(commandCache, times(0)).put(eq("12"), any());
    }

    @Test
    public void handleResponse() {
        AtomicBoolean responseHandled = new AtomicBoolean(false);
        ClientStreamIdentification client = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "Client");
        CommandInformation commandInformation = new CommandInformation("TheCommand",
                                                                       "Source",
                                                                       "Target",
                                                                       (r) -> responseHandled.set(true),
                                                                       client, "Component");
        when(commandCache.remove(any(String.class))).thenReturn(commandInformation);

        commandDispatcher.handleResponse(new SerializedCommandResponse(CommandResponse.newBuilder().build()), false);
        assertTrue(responseHandled.get());
    }

    @Test
    public void dispatchRequestRejected() throws ExecutionException, InterruptedException {
        commandDispatcher = new CommandDispatcher(registrations, commandCache, metricsRegistry, meterFactory,
                                                  new CommandInterceptors() {
                                                      @Override
                                                      public SerializedCommand commandRequest(
                                                              SerializedCommand serializedCommand,
                                                              ExtensionUnitOfWork extensionUnitOfWork) {
                                                          throw new MessagingPlatformException(ErrorCode.COMMAND_REJECTED_BY_INTERCEPTOR,
                                                                                               "failed");
                                                      }

                                                      @Override
                                                      public SerializedCommandResponse commandResponse(
                                                              SerializedCommandResponse serializedResponse,
                                                              ExtensionUnitOfWork extensionUnitOfWork) {
                                                          return serializedResponse;
                                                      }
                                                  }, 10_000);
        CompletableFuture<SerializedCommandResponse> futureResponse = new CompletableFuture<>();
        commandDispatcher.dispatch("demo",
                                   null,
                                   new SerializedCommand(Command.newBuilder().setMessageIdentifier("1234").build()),
                                   r -> futureResponse.complete(r));
        SerializedCommandResponse response = futureResponse.get();
        assertEquals(ErrorCode.COMMAND_REJECTED_BY_INTERCEPTOR.getCode(), response.getErrorCode());
        assertEquals("1234", response.getRequestIdentifier());
    }

    @Test
    public void dispatchRequestInterceptorException() throws ExecutionException, InterruptedException {
        commandDispatcher = new CommandDispatcher(registrations, commandCache, metricsRegistry, meterFactory,
                                                  new CommandInterceptors() {
                                                      @Override
                                                      public SerializedCommand commandRequest(
                                                              SerializedCommand serializedCommand,
                                                              ExtensionUnitOfWork extensionUnitOfWork) {
                                                          throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                                               "failed");
                                                      }

                                                      @Override
                                                      public SerializedCommandResponse commandResponse(
                                                              SerializedCommandResponse serializedResponse,
                                                              ExtensionUnitOfWork extensionUnitOfWork) {
                                                          return serializedResponse;
                                                      }
                                                  }, 10_000);
        CompletableFuture<SerializedCommandResponse> futureResponse = new CompletableFuture<>();
        commandDispatcher.dispatch("demo",
                                   null,
                                   new SerializedCommand(Command.newBuilder().setMessageIdentifier("1234").build()),
                                   r -> futureResponse.complete(r));
        SerializedCommandResponse response = futureResponse.get();
        assertEquals(ErrorCode.EXCEPTION_IN_INTERCEPTOR.getCode(), response.getErrorCode());
        assertEquals("1234", response.getRequestIdentifier());
    }

    @Test
    public void handleResponseInterceptorException() throws ExecutionException, InterruptedException {
        commandCache = new CommandCache(10000, Clock.systemUTC(), 100000);
        FakeStreamObserver<SerializedCommandProviderInbound> commandProviderInbound = new FakeStreamObserver<>();
        ClientStreamIdentification client = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client");
        DirectCommandHandler result = new DirectCommandHandler(commandProviderInbound,
                                                               client, "client", "component");
        when(registrations.getHandlerForCommand(eq(Topology.DEFAULT_CONTEXT), any(), any())).thenReturn(result);
        commandDispatcher = new CommandDispatcher(registrations, commandCache, metricsRegistry, meterFactory,
                                                  new CommandInterceptors() {
                                                      @Override
                                                      public SerializedCommand commandRequest(
                                                              SerializedCommand serializedCommand,
                                                              ExtensionUnitOfWork extensionUnitOfWork) {
                                                          return serializedCommand;
                                                      }

                                                      @Override
                                                      public SerializedCommandResponse commandResponse(
                                                              SerializedCommandResponse serializedResponse,
                                                              ExtensionUnitOfWork extensionUnitOfWork) {
                                                          throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                                               "failed");
                                                      }
                                                  }, 10_000);

        CompletableFuture<SerializedCommandResponse> futureResponse = new CompletableFuture<>();
        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, null,
                                   new SerializedCommand(Command.newBuilder()
                                                                .setMessageIdentifier("TheCommand")
                                                                .build()), futureResponse::complete);


        commandDispatcher.handleResponse(new SerializedCommandResponse(CommandResponse.newBuilder()
                                                                                      .setRequestIdentifier("TheCommand")
                                                                                      .build()), false);
        SerializedCommandResponse response = futureResponse.get();
        assertEquals(ErrorCode.EXCEPTION_IN_INTERCEPTOR.getCode(), response.getErrorCode());
    }
}
