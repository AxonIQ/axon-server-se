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
import io.axoniq.axonserver.topology.DefaultTopology;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class PlatformServiceTest {
    private PlatformService platformService;

    private Topology clusterController;
    @Before
    public void setUp()  {
        MessagingPlatformConfiguration configuration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        clusterController = new DefaultTopology(configuration);
        platformService = new PlatformService(clusterController,
                                              () -> Topology.DEFAULT_CONTEXT,
                                              eventPublisher,
                                              new DefaultInstructionAckSource<>(ack -> PlatformOutboundInstruction
                                                      .newBuilder().setAck(ack).build()));
    }

    @Test
    public void getPlatformServer() {
        StreamObserver<PlatformInfo> responseObserver = new StreamObserver<PlatformInfo>() {
            @Override
            public void onNext(PlatformInfo platformInfo) {
                System.out.println( platformInfo.getPrimary());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        ClientIdentification client = ClientIdentification.newBuilder().setClientId("client").setComponentName("component").build();
        platformService.getPlatformServer(client, responseObserver);
    }

    @Test
    public void openStream() {
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(new StreamObserver<PlatformOutboundInstruction>() {
            @Override
            public void onNext(PlatformOutboundInstruction platformOutboundInstruction) {
                System.out.println( platformOutboundInstruction);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                .setClientId("client")
                .setComponentName("component")
                ).build());

        platformService.requestReconnect("client");
    }

    @Test
    public void unsupportedInstruction() {
        CountingStreamObserver<PlatformOutboundInstruction> responseStream = new CountingStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseStream);

        String instructionId = "instructionId";
        requestStream.onNext(PlatformInboundInstruction.newBuilder()
                                                       .setInstructionId(instructionId)
                                                       .build());

        InstructionAck ack = responseStream.responseList.get(responseStream.responseList.size() - 1).getAck();
        assertEquals(instructionId, ack.getInstructionId());
        assertTrue(ack.hasError());
        assertEquals(ErrorCode.UNSUPPORTED_INSTRUCTION.getCode(), ack.getError().getErrorCode());
    }

    @Test
    public void unsupportedInstructionWithoutInstructionId() {
        CountingStreamObserver<PlatformOutboundInstruction> responseStream = new CountingStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseStream);

        requestStream.onNext(PlatformInboundInstruction.newBuilder().build());

        assertEquals(0, responseStream.responseList.size());
    }

    @Test
    public void onPauseEventProcessorRequest() {
        CountingStreamObserver<PlatformOutboundInstruction> responseObserver = new CountingStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseObserver);
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId("Release")
                                                                                                     .setComponentName("component")
        ).build());
        platformService.onPauseEventProcessorRequest(new EventProcessorEvents.PauseEventProcessorRequest("Release", "processor", false));
        assertEquals(1, responseObserver.count);
    }

    @Test
    public void onStartEventProcessorRequest() {
        CountingStreamObserver<PlatformOutboundInstruction> responseObserver = new CountingStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseObserver);
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId("Release")
                                                                                                     .setComponentName("component")
        ).build());
        platformService.onStartEventProcessorRequest(new EventProcessorEvents.StartEventProcessorRequest("Release", "processor", false));
        assertEquals(1, responseObserver.count);
    }

    @Test
    public void onMergeSegmentRequest() {
        CountingStreamObserver<PlatformOutboundInstruction> responseObserver = new CountingStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseObserver);
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId("MergeClient")
                                                                                                     .setComponentName("component")
        ).build());
        EventProcessorEvents.MergeSegmentRequest mergeSegmentRequest =
                new EventProcessorEvents.MergeSegmentRequest(false,
                                                             "MergeClient",
                                                             "Processor",
                                                             1);
        platformService.on(mergeSegmentRequest);

        assertEquals(1, responseObserver.count);
    }

    @Test
    public void onSplitSegmentRequest() {
        CountingStreamObserver<PlatformOutboundInstruction> responseObserver = new CountingStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseObserver);
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId("MergeClient")
                                                                                                     .setComponentName("component")
        ).build());
        CountingStreamObserver<PlatformOutboundInstruction> responseObserver2 = new CountingStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream2 = platformService.openStream(responseObserver2);
        requestStream2.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId("SplitClient")
                                                                                                     .setComponentName("component")
        ).build());
        EventProcessorEvents.SplitSegmentRequest splitSegmentRequest =
                new EventProcessorEvents.SplitSegmentRequest(false, "SplitClient", "processor", 1);
        platformService.on(splitSegmentRequest);
        assertEquals(0, responseObserver.count);
        assertEquals(1, responseObserver2.count);
    }

    @Test
    public void onInboundInstruction() {
        AtomicBoolean eventProcessorInfoReceived = new AtomicBoolean();
        platformService.onInboundInstruction(PlatformInboundInstruction.RequestCase.EVENT_PROCESSOR_INFO,
                                             (client, context, instruction) -> eventProcessorInfoReceived.set(true));
        StreamObserver<PlatformInboundInstruction> clientStreamObserver = platformService
                .openStream(new CountingStreamObserver<>());
        clientStreamObserver.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId("MergeClient")
                                                                                                     .setComponentName("component")
        ).build());
        clientStreamObserver.onNext(PlatformInboundInstruction.newBuilder().setEventProcessorInfo(EventProcessorInfo.getDefaultInstance()).build());
        assertTrue(eventProcessorInfoReceived.get());
    }

    @Test
    public void onReleaseSegmentRequest() {
        CountingStreamObserver<PlatformOutboundInstruction> responseObserver = new CountingStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseObserver);
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId("Release")
                                                                                                     .setComponentName("component")
        ).build());
        platformService.on(new EventProcessorEvents.ReleaseSegmentRequest("Release", "processor", 1, false));
        assertEquals(1, responseObserver.count);
    }

    @Test
    public void onApplicationDisconnected() {
        CountingStreamObserver<PlatformOutboundInstruction> responseObserver = new CountingStreamObserver<>();
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(responseObserver);
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                                                                                                     .setClientId("Release")
                                                                                                     .setComponentName("component")
        ).build());
        assertEquals(1, platformService.getConnectedClients().size());
        platformService.on(new TopologyEvents.ApplicationDisconnected(Topology.DEFAULT_CONTEXT, "component", "Release"));
        assertEquals(0, platformService.getConnectedClients().size());
        assertTrue(responseObserver.completed);
    }

}
