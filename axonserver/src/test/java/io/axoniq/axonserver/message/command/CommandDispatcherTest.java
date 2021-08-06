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
import io.axoniq.axonserver.component.command.FakeCommandHandler;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.interceptor.CommandInterceptors;
import io.axoniq.axonserver.interceptor.NoOpCommandInterceptors;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;

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

    public static final int COMMAND_TIMEOUT = 10000;
    private CommandDispatcher commandDispatcher;
    MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());
    private CommandMetricsRegistry metricsRegistry;
    @Mock
    private CommandRegistrationCache registrations;
    private final AtomicBoolean tooManyRequests = new AtomicBoolean();
    private final CapacityValidator sampleCapacityValidator = context -> {
        if(tooManyRequests.get()) {
            throw new InsufficientBufferCapacityException("Too many requests");
        }
        return () -> {};
    };

    @Before
    public void setup() {
        metricsRegistry = new CommandMetricsRegistry(meterFactory);
        commandDispatcher = new CommandDispatcher(registrations, metricsRegistry,
                                                  new NoOpCommandInterceptors(), sampleCapacityValidator, COMMAND_TIMEOUT);
    }

    @Test
    public void dispatch() {
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        ClientStreamIdentification client = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client");
        FakeCommandHandler result = new FakeCommandHandler(
                client, "client", "component");
        when(registrations.getHandlerForCommand(eq(Topology.DEFAULT_CONTEXT), any(), any())).thenReturn(result);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT,
                                   GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                   new SerializedCommand(request))
        .subscribe(response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
        assertEquals(1, result.requests());
        assertEquals(0, responseObserver.values().size());
//        Mockito.verify(commandCache, times(1)).put(eq("12"), any());
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
                                   new SerializedCommand(request)).subscribe(
                                   response -> {
                                       responseObserver.onNext(response);
                                       responseObserver.onCompleted();
                                   });
        assertEquals(1, responseObserver.values().size());
        assertNotEquals("", responseObserver.values().get(0).getErrorCode());
    }

    @Test
    public void dispatchQueueFull() {
        tooManyRequests.set(true);
        commandDispatcher = new CommandDispatcher(registrations,
                                                  metricsRegistry,
                                                  new NoOpCommandInterceptors(), sampleCapacityValidator, COMMAND_TIMEOUT);
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        ClientStreamIdentification client = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client");
        CommandHandler result = new FakeCommandHandler( client, "client","component");
        when(registrations.getHandlerForCommand(any(), any(), any())).thenReturn(result);
        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT,
                                   GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                   new SerializedCommand(request)).subscribe(
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
                                   new SerializedCommand(request)).subscribe(
                                   response -> {
                                       responseObserver.onNext(response);
                                       responseObserver.onCompleted();
                                   });
        assertEquals(1, responseObserver.values().size());
        assertEquals("AXONIQ-4000", responseObserver.values().get(0).getErrorCode());
    }

    @Test
    public void dispatchProxied() {
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        Command request = Command.newBuilder()
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        ClientStreamIdentification clientIdentification = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                         "client");
        FakeCommandHandler result = new FakeCommandHandler(
                clientIdentification,
                "client",
                "component");
        when(registrations.findByClientAndCommand(eq(clientIdentification), any())).thenReturn(result);

        commandDispatcher.dispatchProxied(Topology.DEFAULT_CONTEXT,
                                          new SerializedCommand(request.toByteArray(),
                                                                "client",
                                                                request.getMessageIdentifier())).subscribe(
                                          responseObserver::onNext);
        assertEquals(1, result.requests());
        assertEquals(0, responseObserver.values().size());
//        Mockito.verify(commandCache, times(1)).put(eq("12"), any());
    }

    @Test
    public void dispatchProxiedClientNotFound() {
        FakeStreamObserver<SerializedCommandResponse> responseObserver = new FakeStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();

        commandDispatcher.dispatchProxied(Topology.DEFAULT_CONTEXT,
                                          new SerializedCommand(request)).subscribe(
                                          responseObserver::onNext);
        assertEquals(1, responseObserver.values().size());
//        Mockito.verify(commandCache, times(0)).put(eq("12"), any());
    }

    @Test
    public void dispatchRequestRejected() throws ExecutionException, InterruptedException {
        commandDispatcher = new CommandDispatcher(registrations, metricsRegistry,
                                                  new CommandInterceptors() {
                                                      @Override
                                                      public SerializedCommand commandRequest(
                                                              SerializedCommand serializedCommand,
                                                              ExecutionContext executionContext) {
                                                          throw new MessagingPlatformException(ErrorCode.COMMAND_REJECTED_BY_INTERCEPTOR,
                                                                                               "failed");
                                                      }

                                                      @Override
                                                      public SerializedCommandResponse commandResponse(
                                                              SerializedCommandResponse serializedResponse,
                                                              ExecutionContext executionContext) {
                                                          return serializedResponse;
                                                      }
                                                  }, sampleCapacityValidator, 10000);
        CompletableFuture<SerializedCommandResponse> futureResponse = new CompletableFuture<>();
        commandDispatcher.dispatch("demo",
                                   null,
                                   new SerializedCommand(Command.newBuilder().setMessageIdentifier("1234").build())).subscribe(
                                   futureResponse::complete);
        SerializedCommandResponse response = futureResponse.get();
        assertEquals(ErrorCode.COMMAND_REJECTED_BY_INTERCEPTOR.getCode(), response.getErrorCode());
        assertEquals("1234", response.getRequestIdentifier());
    }

    @Test
    public void dispatchRequestInterceptorException() throws ExecutionException, InterruptedException {
        commandDispatcher = new CommandDispatcher(registrations, metricsRegistry,
                                                  new CommandInterceptors() {
                                                      @Override
                                                      public SerializedCommand commandRequest(
                                                              SerializedCommand serializedCommand,
                                                              ExecutionContext executionContext) {
                                                          throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                                               "failed");
                                                      }

                                                      @Override
                                                      public SerializedCommandResponse commandResponse(
                                                              SerializedCommandResponse serializedResponse,
                                                              ExecutionContext executionContext) {
                                                          return serializedResponse;
                                                      }
                                                  }, sampleCapacityValidator, COMMAND_TIMEOUT);
        CompletableFuture<SerializedCommandResponse> futureResponse = new CompletableFuture<>();
        commandDispatcher.dispatch("demo",
                                   null,
                                   new SerializedCommand(Command.newBuilder().setMessageIdentifier("1234").build())).subscribe(
                                   futureResponse::complete);
        SerializedCommandResponse response = futureResponse.get();
        assertEquals(ErrorCode.EXCEPTION_IN_INTERCEPTOR.getCode(), response.getErrorCode());
        assertEquals("1234", response.getRequestIdentifier());
    }

    @Test
    public void handleResponseInterceptorException() throws ExecutionException, InterruptedException {
        ClientStreamIdentification client = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT, "client");
        FakeCommandHandler result = new FakeCommandHandler(client, "client", "component");
        when(registrations.getHandlerForCommand(eq(Topology.DEFAULT_CONTEXT), any(), any())).thenReturn(result);
        commandDispatcher = new CommandDispatcher(registrations, metricsRegistry,
                                                  new CommandInterceptors() {
                                                      @Override
                                                      public SerializedCommand commandRequest(
                                                              SerializedCommand serializedCommand,
                                                              ExecutionContext executionContext) {
                                                          return serializedCommand;
                                                      }

                                                      @Override
                                                      public SerializedCommandResponse commandResponse(
                                                              SerializedCommandResponse serializedResponse,
                                                              ExecutionContext executionContext) {
                                                          throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                                               "failed");
                                                      }
                                                  }, sampleCapacityValidator, COMMAND_TIMEOUT);

        CompletableFuture<SerializedCommandResponse> futureResponse = new CompletableFuture<>();
        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, null,
                                   new SerializedCommand(Command.newBuilder()
                                                                .setMessageIdentifier("TheCommand")
                                                                .build()))
                        .subscribe(futureResponse::complete);


        result.commandResponse(new SerializedCommandResponse(CommandResponse.newBuilder()
                                                                                      .setRequestIdentifier("TheCommand")
                                                                                      .build()));
        SerializedCommandResponse response = futureResponse.get();
        assertEquals(ErrorCode.EXCEPTION_IN_INTERCEPTOR.getCode(), response.getErrorCode());
    }
}
