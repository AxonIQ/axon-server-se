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
import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.DefaultTopology;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.junit.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class PlatformServiceTest {

    private final String context = Topology.DEFAULT_CONTEXT;
    private PlatformService platformService;
    private ClientIdRegistry clientIdRegistry = new DefaultClientIdRegistry();

    @Before
    public void setUp() {
        MessagingPlatformConfiguration configuration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        platformService = new PlatformService(new DefaultTopology(configuration),
                                              () -> Topology.DEFAULT_CONTEXT,
                                              clientIdRegistry, eventPublisher,
                                              new DefaultInstructionAckSource<>(ack -> PlatformOutboundInstruction
                                                      .newBuilder().setAck(ack).build()));
    }

    @Test
    public void getPlatformServer() {
        StreamObserver<PlatformInfo> responseObserver = new StreamObserver<PlatformInfo>() {
            @Override
            public void onNext(PlatformInfo platformInfo) {
                System.out.println(platformInfo.getPrimary());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        ClientIdentification client = ClientIdentification.newBuilder().setClientId("client").setComponentName(
                "component").build();
        platformService.getPlatformServer(client, responseObserver);
    }

    @Test
    public void openStream() {
        StreamObserver<PlatformInboundInstruction> requestStream = platformService
                .openStream(new StreamObserver<PlatformOutboundInstruction>() {
                    @Override
                    public void onNext(PlatformOutboundInstruction platformOutboundInstruction) {
                        System.out.println(platformOutboundInstruction);
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onCompleted() {

                    }
                });
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId(
                                                                                                             "client")
                                                                                                     .setComponentName(
                                                                                                             "component")
        ).build());

        platformService.requestReconnect("client");
    }

    @Test
    public void unsupportedInstruction() {
        FakeStreamObserver<PlatformOutboundInstruction> responseStream = new FakeStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseStream);

        String instructionId = "instructionId";
        requestStream.onNext(PlatformInboundInstruction.newBuilder()
                                                       .setInstructionId(instructionId)
                                                       .build());

        InstructionAck ack = responseStream.values().get(responseStream.values().size() - 1).getAck();
        assertEquals(instructionId, ack.getInstructionId());
        assertTrue(ack.hasError());
        assertEquals(ErrorCode.UNSUPPORTED_INSTRUCTION.getCode(), ack.getError().getErrorCode());
    }

    @Test
    public void unsupportedInstructionWithoutInstructionId() {
        FakeStreamObserver<PlatformOutboundInstruction> responseStream = new FakeStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseStream);

        requestStream.onNext(PlatformInboundInstruction.newBuilder().build());

        assertEquals(0, responseStream.values().size());
    }

    @Test
    public void onPauseEventProcessorRequest() {
        FakeStreamObserver<PlatformOutboundInstruction> responseObserver = new FakeStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseObserver);
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId(
                                                                                                             "Release")
                                                                                                     .setComponentName(
                                                                                                             "component")
        ).build());
        platformService.on(new EventProcessorEvents.PauseEventProcessorRequest(Topology.DEFAULT_CONTEXT,
                                                                               "Release",
                                                                               "processor",
                                                                               false));

        assertEquals(1, responseObserver.values().size());
    }

    @Test
    public void onStartEventProcessorRequest() {
        FakeStreamObserver<PlatformOutboundInstruction> responseObserver = new FakeStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseObserver);
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId(
                                                                                                             "Release")
                                                                                                     .setComponentName(
                                                                                                             "component")
        ).build());

        platformService.on(new EventProcessorEvents.StartEventProcessorRequest(Topology.DEFAULT_CONTEXT,
                                                                               "Release",
                                                                               "processor",
                                                                               false));
        assertEquals(1, responseObserver.values().size());
    }

    @Test
    public void onInboundInstruction() {
        AtomicBoolean eventProcessorInfoReceived = new AtomicBoolean();
        platformService.onInboundInstruction(PlatformInboundInstruction.RequestCase.EVENT_PROCESSOR_INFO,
                                             (client, instruction) -> eventProcessorInfoReceived.set(true));
        StreamObserver<PlatformInboundInstruction> clientStreamObserver = platformService
                .openStream(new FakeStreamObserver<>());
        clientStreamObserver.onNext(PlatformInboundInstruction.newBuilder()
                                                              .setRegister(ClientIdentification.newBuilder()
                                                                                               .setClientId(
                                                                                                       "MergeClient")
                                                                                               .setComponentName(
                                                                                                       "component")
                                                              ).build());
        clientStreamObserver.onNext(PlatformInboundInstruction.newBuilder().setEventProcessorInfo(EventProcessorInfo
                                                                                                          .getDefaultInstance())
                                                              .build());
        assertTrue(eventProcessorInfoReceived.get());
    }

    @Test
    public void onApplicationDisconnected() {
        FakeStreamObserver<PlatformOutboundInstruction> responseObserver = new FakeStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseObserver);
        String component = "component";
        String clientId = "Release";
        requestStream.onNext(PlatformInboundInstruction.newBuilder()
                                                       .setRegister(ClientIdentification.newBuilder()
                                                                                        .setClientId(clientId)
                                                                                        .setComponentName(component)
                                                       ).build());
        assertEquals(1, platformService.getConnectedClients().size());
        String clientStreamId = platformService.getConnectedClients().iterator().next().getClientStreamId();
        platformService.on(new TopologyEvents.ApplicationDisconnected(Topology.DEFAULT_CONTEXT,
                                                                      component, clientStreamId));
        assertEquals(0, platformService.getConnectedClients().size());
        assertTrue(responseObserver.completedCount() == 1);
    }

    @Test
    public void testSendInstruction() {
        PlatformOutboundInstruction i = PlatformOutboundInstruction.newBuilder().build();
        FakeStreamObserver<PlatformOutboundInstruction> responseObserver = new FakeStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> clientStreamObserver = platformService.openStream(responseObserver);
        clientStreamObserver.onNext(PlatformInboundInstruction
                                            .newBuilder()
                                            .setRegister(ClientIdentification.newBuilder()
                                                                             .setClientId("MyClient")
                                                                             .setComponentName("component")).build());
        platformService.sendToClient("default", "MyClient", i);
        assertEquals(1, responseObserver.values().size());
        assertEquals(i, responseObserver.values().get(0));
    }

    @Test
    public void testSendInstructionToInvalidClientIdentifier() {
        PlatformOutboundInstruction i = PlatformOutboundInstruction.newBuilder().build();
        FakeStreamObserver<PlatformOutboundInstruction> responseObserver = new FakeStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> clientStreamObserver = platformService.openStream(responseObserver);
        clientStreamObserver.onNext(PlatformInboundInstruction
                                            .newBuilder()
                                            .setRegister(ClientIdentification.newBuilder()
                                                                             .setClientId("MyClient")
                                                                             .setComponentName("component")).build());
        platformService.sendToClient("wrong-context", "MyClient", i);
        assertEquals(0, responseObserver.values().size());
    }

    @Test
    public void disconnectClientStream() {
        FakeStreamObserver<PlatformOutboundInstruction> responseObserver = new FakeStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> clientStreamObserver = platformService.openStream(responseObserver);
        String component = "component";
        String clientId = "MyClient";
        PlatformInboundInstruction registercommand = PlatformInboundInstruction
                .newBuilder()
                .setRegister(ClientIdentification.newBuilder()
                                                 .setClientId(clientId)
                                                 .setComponentName(component))
                .build();
        clientStreamObserver.onNext(registercommand);
        assertEquals(1, platformService.getConnectedClients().size());
        String clientStreamId = platformService.getConnectedClients().iterator().next().getClientStreamId();
        ClientStreamIdentification client = new ClientStreamIdentification("default", clientStreamId);
        platformService.on(new TopologyEvents.ApplicationInactivityTimeout(client, component, clientId));
        assertEquals(1, responseObserver.errors().size());
        assertTrue(responseObserver.errors().get(0).getMessage().contains("Platform stream inactivity"));
    }
}
