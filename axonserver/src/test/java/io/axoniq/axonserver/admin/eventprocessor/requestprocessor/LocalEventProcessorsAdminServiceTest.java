/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

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
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;

/**
 * @author Sara Pellegrini
 */
@RunWith(MockitoJUnitRunner.class)
public class LocalEventProcessorsAdminServiceTest {

    @Mock
    ProcessorEventPublisher publisher;

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
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors, strategyController);
        testSubject.pause(new EventProcessorIdentifier(processorName, tokenStore), () -> "authenticated-user")
                   .block();
        verify(publisher).pauseProcessorRequest("default", "Client-A", processorName);
        verify(publisher).pauseProcessorRequest("default", "Client-B", processorName);
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
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors, strategyController);
        testSubject.start(new EventProcessorIdentifier(processorName, tokenStore), () -> "authenticated-user")
                   .block();
        verify(publisher).startProcessorRequest("default", "Client-A", processorName);
        verify(publisher).startProcessorRequest("default", "Client-B", processorName);
        verifyNoMoreInteractions(publisher);
    }


    @Test
    public void splitTest() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors, strategyController);
        testSubject.split(new EventProcessorIdentifier(processorName, tokenStore), () -> "authenticated-user")
                   .block();
        List<String> clients = asList("Client-A", "Client-B");
        verify(publisher).splitSegment("default", clients, processorName);
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void mergeTest() {
        String processorName = "processorName";
        String tokenStore = "tokenStore";
        ClientProcessor clientA = new FakeClientProcessor("Client-A", processorName, tokenStore);
        ClientProcessor clientB = new FakeClientProcessor("Client-B", processorName, tokenStore);
        ClientProcessor clientC = new FakeClientProcessor("Client-C", "anotherProcessor", tokenStore);
        ClientProcessor clientD = new FakeClientProcessor("Client-D", processorName, "anotherTokenStore");
        ClientProcessors processors = () -> asList(clientA, clientB, clientC, clientD).iterator();
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors, strategyController);
        testSubject.merge(new EventProcessorIdentifier(processorName, tokenStore), () -> "authenticated-user")
                   .block();
        List<String> clients = asList("Client-A", "Client-B");
        verify(publisher).mergeSegment("default", clients, processorName);
        verifyNoMoreInteractions(publisher);
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
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors, strategyController);
        testSubject.move(new EventProcessorIdentifier(processorName, tokenStore), 2, "Client-B",
                         () -> "authenticated-user").block();
        verify(publisher).releaseSegment("default", "Client-A", processorName, 2);
        verify(publisher).releaseSegment("default", "Client-E", processorName, 2);
        verifyNoMoreInteractions(publisher);
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
        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors, strategyController);
        Flux<String> clients = testSubject.eventProcessorsByComponent("component",
                                                                      () -> "authenticated-user")
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

        LocalEventProcessorsAdminService testSubject = new LocalEventProcessorsAdminService(publisher, processors, strategyController);

        StepVerifier.create(testSubject.loadBalance(processorName, tokenStore, strategy, () -> "authenticated-user"))
                .verifyComplete();

        TrackingEventProcessor tep = tepCaptor.getValue();
        Assertions.assertEquals(tep.fullName(), processorName+"@"+tokenStore);
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
}