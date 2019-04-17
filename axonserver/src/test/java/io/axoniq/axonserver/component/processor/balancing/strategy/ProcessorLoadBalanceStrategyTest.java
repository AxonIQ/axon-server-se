/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import org.junit.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * @author Marc Gathier
 */
public class ProcessorLoadBalanceStrategyTest {

    private ProcessorLoadBalanceStrategy testSubject;

    @Before
    public void setUp()  {
        Map<String, LoadBalancingStrategy.Factory> factories =
                new HashMap<>();
        factories.put("NoLoadBalance", new NoLoadBalanceStrategy.Factory());
        ProcessorEventPublisher processorEventsSource = mock(ProcessorEventPublisher.class);
        ClientProcessors processors = Collections::emptyListIterator;
        factories.put("ThreadNumberBalancingStrategy",
                      new ThreadNumberBalancing.ThreadNumberBalancingStrategyFactory(processorEventsSource, processors));
        testSubject = new ProcessorLoadBalanceStrategy(new SimpleLoadBalanceStrategyHolder(), factories);
    }

    @Test
    public void balanceThreadNumber() {
        testSubject.balance(new TrackingEventProcessor("testProcessor", "testComponent", "default"), "threadNumber").perform();
    }

    @Test
    public void balanceDefault() {
        testSubject.balance(new TrackingEventProcessor("testProcessor", "testComponent", "default"), "default").perform();
    }

}