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
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandDispatcherTest {
    private CommandDispatcher commandDispatcher;
    @Mock
    private CommandCache commandCache;
    @Mock
    private CommandRegistrationCache registrations;
    private FlowControlQueues<WrappedCommand> commandQueue;

    @Before
    public void setup() {
        CommandMetricsRegistry metricsRegistry = new CommandMetricsRegistry(new MeterFactory(new SimpleMeterRegistry(),
                                                                                             new DefaultMetricCollector()));
        commandDispatcher = new CommandDispatcher(registrations, commandCache, metricsRegistry);
        commandQueue = new FlowControlQueues<>();
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
        ClientIdentification client = new ClientIdentification(Topology.DEFAULT_CONTEXT, "client");
        DirectCommandHandler result = new DirectCommandHandler(client, "component",
                                                               commandQueue);
        when(registrations.getHandlerForCommand(eq(Topology.DEFAULT_CONTEXT), any(), anyString())).thenReturn(result);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, new SerializedCommand(request), response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }, false);
        assertEquals(1, commandQueue.getSegments().get(client.toString()).size());
        assertEquals(0, responseObserver.count);
        Mockito.verify(commandCache, times(1)).put(eq("12"), any());

    }
    @Test
    public void dispatchNotFound() {
        CountingStreamObserver<SerializedCommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                .setName("Command")
                .setMessageIdentifier("12")
                .build();
        when(registrations.getHandlerForCommand(any(), any(), anyString())).thenReturn(null);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, new SerializedCommand(request), response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }, false);
        assertEquals(1, responseObserver.count);
        assertNotEquals("", responseObserver.responseList.get(0).getErrorCode());
        Mockito.verify(commandCache, times(0)).put(eq("12"), any());

    }

    @Test
    public void dispatchUnknownContext() {
        CountingStreamObserver<SerializedCommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                                 .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        when(registrations.getHandlerForCommand(anyString(), any(), anyString())).thenReturn(null);

        commandDispatcher.dispatch("UnknownContext", new SerializedCommand(request), response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }, false);
        assertEquals(1, responseObserver.count);
        assertEquals("AXONIQ-4000", responseObserver.responseList.get(0).getErrorCode());
        Mockito.verify(commandCache, times(0)).put(eq("12"), any());

    }

    @Test
    public void dispatchProxied() throws Exception {
        CountingStreamObserver<SerializedCommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                                 .setName("Command")
                                 .setMessageIdentifier("12")
                                 .build();
        ClientIdentification clientIdentification = new ClientIdentification(Topology.DEFAULT_CONTEXT, "client");
        DirectCommandHandler result = new DirectCommandHandler(clientIdentification,
                                                               "component",
                                                               commandQueue);
        when(registrations.findByClientAndCommand(eq(clientIdentification), any())).thenReturn(result);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT,
                                   new SerializedCommand(request.toByteArray(),
                                                         "client",
                                                         request.getMessageIdentifier()),
                                   responseObserver::onNext,
                                   true);
        assertEquals(1, commandQueue.getSegments().get(clientIdentification.toString()).size());
        assertEquals("12", commandQueue.take(clientIdentification.toString()).command().getMessageIdentifier());
        assertEquals(0, responseObserver.count);
        Mockito.verify(commandCache, times(1)).put(eq("12"), any());
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
        Mockito.verify(commandCache, times(0)).put(eq("12"), any());
    }

    @Test
    public void handleResponse() {
        AtomicBoolean responseHandled = new AtomicBoolean(false);
        ClientIdentification client = new ClientIdentification(Topology.DEFAULT_CONTEXT, "Client");
        CommandInformation commandInformation = new CommandInformation("TheCommand",
                                                                       "Source",
                                                                       (r) -> responseHandled.set(true),
                                                                       client, "Component");
        when(commandCache.remove(any(String.class))).thenReturn(commandInformation);

        commandDispatcher.handleResponse(new SerializedCommandResponse(CommandResponse.newBuilder().build()), false);
        assertTrue(responseHandled.get());
//        assertEquals(1, metricsRegistry.commandMetric("TheCommand", client, "Component").getCount());

    }
}
