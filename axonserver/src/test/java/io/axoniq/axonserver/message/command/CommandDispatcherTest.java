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
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;

import java.time.Clock;
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
    private CommandCache commandCache = new CommandCache(1000, Clock.systemUTC());
    @Mock
    private CommandRegistrationCache registrations;

    @Before
    public void setup() {
        metricsRegistry = new CommandMetricsRegistry(meterFactory);
        commandDispatcher = new CommandDispatcher(registrations, commandCache, metricsRegistry, meterFactory, 10_000);
    }

    @Test
    public void unregisterCommandHandler()  {
        commandDispatcher.on(new TopologyEvents.ApplicationDisconnected(null, null, "client"));
    }

    @Test
    public void dispatch()  {
        CountingStreamObserver<SerializedCommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                .setName("Command")
                .setMessageIdentifier("12")
                .build();
        CountingStreamObserver<SerializedCommandProviderInbound> commandProviderInbound = new CountingStreamObserver<>();
        ClientIdentification client = new ClientIdentification(Topology.DEFAULT_CONTEXT, "client");
        DirectCommandHandler result = new DirectCommandHandler(commandProviderInbound,
                                                               client, "component");
        when(registrations.getHandlerForCommand(eq(Topology.DEFAULT_CONTEXT), anyObject(), anyObject())).thenReturn(result);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, new SerializedCommand(request), response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }, false);
        assertEquals(1, commandDispatcher.getCommandQueues().getSegments().get(client.toString()).size());
        assertEquals(0, responseObserver.count);
        assertEquals(1, commandCache.size());

    }
    @Test
    public void dispatchNotFound() {
        CountingStreamObserver<SerializedCommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                .setName("Command")
                .setMessageIdentifier("12")
                .build();
        when(registrations.getHandlerForCommand(any(), anyObject(), anyObject())).thenReturn(null);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, new SerializedCommand(request), response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }, false);
        assertEquals(1, responseObserver.count);
        assertNotEquals("", responseObserver.responseList.get(0).getErrorCode());
        assertTrue(commandCache.isEmpty());
    }

    @Test
    public void dispatchQueueFull() {
        commandDispatcher = new CommandDispatcher(registrations, commandCache, metricsRegistry, meterFactory, 0);
        CountingStreamObserver<SerializedCommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        CountingStreamObserver<SerializedCommandProviderInbound> commandProviderInbound = new CountingStreamObserver<>();
        ClientIdentification client = new ClientIdentification(Topology.DEFAULT_CONTEXT, "client");
        DirectCommandHandler result = new DirectCommandHandler(commandProviderInbound,
                                                               client, "component");
        when(registrations.getHandlerForCommand(any(), anyObject(), anyObject())).thenReturn(result);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, new SerializedCommand(request), response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }, false);
        assertEquals(1, responseObserver.count);
        assertNotEquals("", responseObserver.responseList.get(0).getErrorCode());
        assertTrue(commandCache.isEmpty());
    }

    @Test
    public void dispatchUnknownContext() {
        CountingStreamObserver<SerializedCommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        when(registrations.getHandlerForCommand(any(), anyObject(), anyObject())).thenReturn(null);

        commandDispatcher.dispatch("UnknownContext", new SerializedCommand(request), response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }, false);
        assertEquals(1, responseObserver.count);
        assertEquals("AXONIQ-4000", responseObserver.responseList.get(0).getErrorCode());
    }

    @Test
    public void dispatchProxied() throws Exception {
        CountingStreamObserver<SerializedCommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                .setName("Command")
                .setMessageIdentifier("12")
                .build();
        ClientIdentification clientIdentification = new ClientIdentification(Topology.DEFAULT_CONTEXT,"client");
        CountingStreamObserver<SerializedCommandProviderInbound> commandProviderInbound = new CountingStreamObserver<>();
        DirectCommandHandler result = new DirectCommandHandler(commandProviderInbound, clientIdentification, "component");
        when(registrations.findByClientAndCommand(eq(clientIdentification), anyObject())).thenReturn(result);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, new SerializedCommand(request.toByteArray(), "client", request.getMessageIdentifier()), responseObserver::onNext, true);
        assertEquals(1, commandDispatcher.getCommandQueues().getSegments().get(clientIdentification.toString()).size());
        assertEquals("12", commandDispatcher.getCommandQueues().take(clientIdentification.toString()).command().getMessageIdentifier());
        assertEquals(0, responseObserver.count);
        assertEquals(1, commandCache.size());
    }

    @Test
    public void dispatchProxiedClientNotFound()  {
        CountingStreamObserver<SerializedCommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                .setName("Command")
                .setMessageIdentifier("12")
                .build();

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, new SerializedCommand(request), responseObserver::onNext, true);
        assertEquals(1, responseObserver.count);
        assertTrue(commandCache.isEmpty());
    }

    @Test
    public void handleResponse() {
        AtomicBoolean responseHandled = new AtomicBoolean(false);
        ClientIdentification client = new ClientIdentification(Topology.DEFAULT_CONTEXT, "Client");
        CommandInformation commandInformation = new CommandInformation("TheCommand",
                                                                       "Source",
                                                                       (r) -> responseHandled.set(true),
                                                                       client, "Component");
        commandCache.put(commandInformation.getRequestIdentifier(), commandInformation);

        commandDispatcher.handleResponse(new SerializedCommandResponse(CommandResponse.newBuilder()
                                                                                      .setRequestIdentifier(
                                                                                              commandInformation
                                                                                                      .getRequestIdentifier())
                                                                                      .build()), false);
        assertTrue(responseHandled.get());
//        assertEquals(1, metricsRegistry.commandMetric("TheCommand", client, "Component").getCount());

    }
}
