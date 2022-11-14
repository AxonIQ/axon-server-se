/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import io.axoniq.axonserver.admin.Instruction;
import io.axoniq.axonserver.admin.InstructionCache;
import io.axoniq.axonserver.admin.InstructionResult;
import io.axoniq.axonserver.admin.Result;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorInstance;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.balancing.LoadBalancingOperation;
import io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.strategy.LoadBalanceStrategyRepository;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.AuthenticatedUser;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import javax.annotation.Nonnull;
import java.util.Collections;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Sara Pellegrini
 */
@RunWith(MockitoJUnitRunner.class)
public class LocalEventProcessorsAdminServiceTest {

    @Mock
    ProcessorEventPublisher publisher;
    private final InstructionCache instructionCache = new InstructionCache(100, 1000);

    @Before
    public void setup() {
        doAnswer(invocationOnMock -> {
            String invocationId = invocationOnMock.getArgument(4);
            String clientId = invocationOnMock.getArgument(1);
            Instruction instruction = instructionCache.get(invocationId);
            if (instruction != null) {
                instruction.on(new SuccessInstructionResult(clientId));
            }
            return null;
        }).when(publisher).splitSegment(anyString(), anyString(), anyString(), anyInt(), anyString());
        doAnswer(invocationOnMock -> {
            String invocationId = invocationOnMock.getArgument(4);
            String clientId = invocationOnMock.getArgument(1);
            Instruction instruction = instructionCache.get(invocationId);
            if (instruction != null) {
                instruction.on(new SuccessInstructionResult(clientId));
            }
            return null;
        }).when(publisher).mergeSegment(anyString(), anyString(), anyString(), anyInt(), anyString());
        doAnswer(invocationOnMock -> {
            String invocationId = invocationOnMock.getArgument(3);
            String clientId = invocationOnMock.getArgument(1);
            Instruction instruction = instructionCache.get(invocationId);
            if (instruction != null) {
                instruction.on(new SuccessInstructionResult(clientId));
            }
            return null;
        }).when(publisher).pauseProcessorRequest(anyString(), anyString(), anyString(), anyString());
        doAnswer(invocationOnMock -> {
            String invocationId = invocationOnMock.getArgument(3);
            String clientId = invocationOnMock.getArgument(1);
            Instruction instruction = instructionCache.get(invocationId);
            if (instruction != null) {
                instruction.on(new SuccessInstructionResult(clientId));
            }
            return null;
        }).when(publisher).startProcessorRequest(anyString(), anyString(), anyString(), anyString());
        doAnswer(invocationOnMock -> {
            String invocationId = invocationOnMock.getArgument(4);
            String clientId = invocationOnMock.getArgument(1);
            Instruction instruction = instructionCache.get(invocationId);
            if (instruction != null) {
                instruction.on(new SuccessInstructionResult(clientId));
            }
            return null;
        }).when(publisher).releaseSegment(anyString(), anyString(), anyString(), anyInt(), anyString());
    }

    @Mock
    LoadBalanceStrategyRepository strategyController;

    @Mock
    LoadBalancingStrategy loadBalancingStrategy;

    @Mock
    LoadBalancingOperation loadBalancingOperation;

    @Test
    public void pauseTest() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        testSubject.pause(new EventProcessorIdentifier(processorName, Topology.DEFAULT_CONTEXT, tokenStore),
                          new AuthenticatedUser("authenticated-user"))
                   .block();
        verify(publisher).pauseProcessorRequest(eq("default"), eq("Client-A"), eq(processorName), any());
        verify(publisher).pauseProcessorRequest(eq("default"), eq("Client-B"), eq(processorName), any());
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void startTest() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        testSubject.start(new EventProcessorIdentifier(processorName, Topology.DEFAULT_CONTEXT, tokenStore),
                          new AuthenticatedUser( "authenticated-user"))
                   .block();
        verify(publisher).startProcessorRequest(eq("default"), eq("Client-A"), eq(processorName), any());
        verify(publisher).startProcessorRequest(eq("default"), eq("Client-B"), eq(processorName), any());
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void pauseUnknownProcessor() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessors processors = () -> asList(clientA, clientB).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        StepVerifier.create(testSubject.pause(new EventProcessorIdentifier("anotherProcessor",
                                                                           Topology.DEFAULT_CONTEXT, tokenStore
                                              ),
                                              new AuthenticatedUser( "authenticated-user")))
                    .expectErrorMatches(t -> matchException(t, ErrorCode.EVENT_PROCESSOR_NOT_FOUND))
                    .verify();
    }


    @Test
    public void startUnknownProcessor() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessors processors = () -> asList(clientA, clientB).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        StepVerifier.create(testSubject.start(new EventProcessorIdentifier("anotherProcessor",
                                                                           Topology.DEFAULT_CONTEXT, tokenStore
                                              ),
                                              new AuthenticatedUser( "authenticated-user")))
                    .expectErrorMatches(t -> matchException(t, ErrorCode.EVENT_PROCESSOR_NOT_FOUND))
                    .verify();
    }

    private boolean matchException(Throwable t, ErrorCode expectedErrorCode) {
        return t instanceof MessagingPlatformException && ((MessagingPlatformException) t).getErrorCode().equals(
                expectedErrorCode);
    }

    @Test
    public void splitTest() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore, segment(0, 2));
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore,
                                                          segment(1, 4),
                                                          segment(2, 4));
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        testSubject.split(new EventProcessorIdentifier(processorName, Topology.DEFAULT_CONTEXT, tokenStore),
                          new AuthenticatedUser( "authenticated-user"))
                   .block();
        verify(publisher).splitSegment(eq("default"), eq("Client-A"), eq(processorName), eq(0), any());
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void splitTestNoMatching() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessors processors = Collections::emptyIterator;
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        StepVerifier.create(testSubject.split(new EventProcessorIdentifier(processorName,
                                                                           Topology.DEFAULT_CONTEXT, tokenStore
                                              ),
                                              new AuthenticatedUser( "authenticated-user")))
                    .expectErrorMatches(t -> matchException(t, ErrorCode.EVENT_PROCESSOR_NOT_FOUND))
                    .verify();
    }

    @Test
    public void mergeTest() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore, segment(0, 2));
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore, segment(1, 2));
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        testSubject.merge(new EventProcessorIdentifier(processorName, Topology.DEFAULT_CONTEXT, tokenStore),
                          new AuthenticatedUser( "authenticated-user"))
                   .block();
        // TODO: 10/02/2022 improve checking to ensure only one of client-a, client-b gets merge request and the other gets release request
        verify(publisher).mergeSegment(eq("default"), eq("Client-B"), eq(processorName), eq(1), any());
        verify(publisher).releaseSegment(eq("default"), eq("Client-A"), eq(processorName), eq(0), any());
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void mergeTest2() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore, segment(0, 4));
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore, segment(2, 4));
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore, segment(1, 16));
        ClientProcessor clientD = new FakeClientProcessor("Client-D",
                                                          processorName,
                                                          "anotherTokenStore",
                                                          segment(1, 8));
        ClientProcessor clientE = new FakeClientProcessor("Client-E",
                                                          processorName,
                                                          tokenStore,
                                                          "anotherContext",
                                                          segment(1, 2));
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD, clientE).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        testSubject.merge(new EventProcessorIdentifier(processorName, Topology.DEFAULT_CONTEXT, tokenStore),
                          new AuthenticatedUser( "authenticated-user"))
                   .block();
        // TODO: 10/02/2022 improve checking to ensure only one of client-a, client-b gets merge request and the other gets release request
        verify(publisher).mergeSegment(eq("default"), eq("Client-B"), eq(processorName), eq(2), any());
        verify(publisher).releaseSegment(eq("default"), eq("Client-A"), eq(processorName), eq(0), any());
//        verify(publisher).releaseSegment(eq("anotherContext"), eq("Client-E"), eq(processorName), eq(0), any());
        verifyNoMoreInteractions(publisher);
    }

    private SegmentStatus segment(int segmentId, int onePartOf) {
        return SegmentStatus.newBuilder().setSegmentId(segmentId)
                            .setOnePartOf(onePartOf)
                            .build();
    }

    @Test
    public void testMoveSegment() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessor clientE = new FakeClientProcessor("Client-E", processorName, tokenStore);
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD, clientE).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        testSubject.move(new EventProcessorIdentifier(processorName, Topology.DEFAULT_CONTEXT, tokenStore),
                         2,
                         "Client-B",
                         new AuthenticatedUser( "authenticated-user")).block();
        verify(publisher).releaseSegment(eq("default"), eq("Client-A"), eq(processorName), eq(2), any());
        verify(publisher).releaseSegment(eq("default"), eq("Client-E"), eq(processorName), eq(2), any());
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void testMoveSegmentNoAvailableThreads() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore)
                .withAvailableThreads(0)
                .withActiveThreads(10);
        ClientProcessors processors = () -> asList(clientA, clientB).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        StepVerifier.create(testSubject.move(new EventProcessorIdentifier(processorName,
                                                                          Topology.DEFAULT_CONTEXT, tokenStore
                                             ), 2, "Client-B",
                                             new AuthenticatedUser( "authenticated-user")))
                    .expectErrorMatches(t -> matchException(t, ErrorCode.EVENT_PROCESSOR_MOVE_NO_AVAILBLE_THREADS))
                    .verify();
    }

    @Test
    public void testMoveSegmentWithTargetNotAvailable() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessors processors = () -> singletonList(clientA).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        StepVerifier.create(testSubject.move(new EventProcessorIdentifier(processorName,
                                                                          Topology.DEFAULT_CONTEXT, tokenStore
                                             ), 2, "Client-B",
                                             new AuthenticatedUser( "authenticated-user")))
                    .expectErrorMatches(t -> matchException(t, ErrorCode.EVENT_PROCESSOR_MOVE_UNKNOWN_TARGET))
                    .verify();
    }

    @Test
    public void testMoveSegmentWithNoClientToRequestRelease() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessors processors = () -> singletonList(clientA).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        StepVerifier.create(testSubject.move(new EventProcessorIdentifier(processorName,
                                                                          Topology.DEFAULT_CONTEXT, tokenStore
                                             ), 2, "Client-A",
                                             new AuthenticatedUser( "authenticated-user")))
                    .expectErrorMatches(t -> matchException(t, ErrorCode.OTHER))
                    .verify();
    }

    @Test
    public void testMoveSegmentWhenAlreadyOwned() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        SegmentStatus segmentStatus = SegmentStatus.newBuilder().setSegmentId(2).build();
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore, segmentStatus);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessors processors = () -> asList(clientA, clientB).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        StepVerifier.create(testSubject.move(new EventProcessorIdentifier(processorName,
                                                                          Topology.DEFAULT_CONTEXT, tokenStore
                                             ),
                                             2, "Client-A",
                                             new AuthenticatedUser( "authenticated-user")))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testGetByComponent() {
        EventProcessorInfo eventProcessorA = buildEventProcessorInfo("blue", "X");
        EventProcessorInfo eventProcessorB = buildEventProcessorInfo("blue", "X");
        EventProcessorInfo eventProcessorC = buildEventProcessorInfo("blue", "Y");
        EventProcessorInfo eventProcessorD = buildEventProcessorInfo("green", "X");
        EventProcessorInfo eventProcessorE = buildEventProcessorInfo("green", "Y");
        ClientProcessor clientA = new FakeClientProcessor("Client-A", true, eventProcessorA);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", false, eventProcessorB);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", false, eventProcessorC);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", false, eventProcessorD);
        ClientProcessor clientE = new FakeClientProcessor("Client-E", true, eventProcessorE);

        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD, clientE).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);
        Flux<String> clients = testSubject.eventProcessorsByComponent("component",
                                                                      new AuthenticatedUser( "authenticated-user"))
                                          .flatMap(eventProcessor -> Flux.fromIterable(eventProcessor.instances()))
                                          .map(EventProcessorInstance::clientId);

        StepVerifier.create(clients)
                    .expectNext("Client-A", "Client-B", "Client-E")
                    .verifyComplete();
    }

    @Test
    public void loadBalance() {
        ArgumentCaptor<TrackingEventProcessor> tepCaptor = ArgumentCaptor.forClass(TrackingEventProcessor.class);
        String strategy = "threadNumber";

        when(loadBalancingStrategy.balance(tepCaptor.capture())).thenReturn(loadBalancingOperation);
        when(strategyController.findByName("threadNumber")).thenReturn(loadBalancingStrategy);

        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessor clientE = new FakeClientProcessor("Client-E", processorName, tokenStore);
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD, clientE).iterator();

        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher,
                                                                                            processors,
                                                                                            instructionCache,
                                                                                            strategyController);

        StepVerifier.create(testSubject.loadBalance(new EventProcessorIdentifier(processorName,
                                                                                 Topology.DEFAULT_CONTEXT, tokenStore
                                                    ),
                                                    strategy,
                                                    new AuthenticatedUser( "authenticated-user")))
                    .verifyComplete();

        TrackingEventProcessor tep = tepCaptor.getValue();
        Assertions.assertEquals(tep.fullName(), processorName + "@" + tokenStore);
        Assertions.assertEquals(tep.context(), "default");
        Assertions.assertEquals(tep.tokenStoreIdentifier(), tokenStore);
        verify(loadBalancingStrategy, times(3)).balance(any(TrackingEventProcessor.class));
        verify(loadBalancingOperation, times(3)).perform();
    }

    @Nonnull
    private EventProcessorInfo buildEventProcessorInfo(String processorName, String tokenStoreId) {
        return EventProcessorInfo.newBuilder()
                                 .setProcessorName(processorName)
                                 .setTokenStoreIdentifier(tokenStoreId)
                                 .build();
    }

    private static class SuccessInstructionResult implements InstructionResult {

        private final String clientId;

        public SuccessInstructionResult(String clientId) {
            this.clientId = clientId;
        }

        @Override
        public String clientId() {
            return clientId;
        }

        @Override
        public Result result() {
            return Result.SUCCESS;
        }

        @Override
        public String errorCode() {
            return null;
        }

        @Override
        public String errorMessage() {
            return null;
        }
    }
}