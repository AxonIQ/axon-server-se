/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.FakeOperationFactory;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.strategy.ThreadNumberBalancing.Application;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.max;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link ThreadNumberBalancing}
 *
 * @author Sara Pellegrini
 */
public class ThreadNumberBalancingTest {

    private final Map<String, Collection<Integer>> segments = new HashMap<>();
    private final Map<String, Integer> threadPoolSize = new HashMap<>();
    private final TrackingEventProcessor eventProcessor = new TrackingEventProcessor("name",
                                                                                     "context",
                                                                                     "tokenStoreId");
    private final ThreadNumberBalancing.InstancesRepo instances = processor -> () -> segments.entrySet().stream().map(e -> {
        int maxThreads = max(threadPoolSize.getOrDefault(e.getKey(), 1), e.getValue().size());
        return new Application(e.getKey(), maxThreads, e.getValue());
    }).iterator();

    private final ThreadNumberBalancing testSubject = new ThreadNumberBalancing(new FakeOperationFactory(segments), instances);

    @Before
    public void setUp() {
        segments.clear();
        threadPoolSize.clear();
    }

    @Test
    public void testTwoSegmentForTwoInstances() {
        segments.put("instanceOne", of(0, 1));
        segments.put("instanceTwo", of());
        testSubject.balance(eventProcessor).perform();
        assertEquals(1, segments.get("instanceOne").size());
        assertEquals(1, segments.get("instanceTwo").size());
    }

    @Test
    public void testFourSegmentForThreeInstances() {
        segments.put("instanceOne", of(0, 1, 3, 4));
        segments.put("instanceTwo", of());
        segments.put("instanceThree", of());
        testSubject.balance(eventProcessor).perform();
        assertEquals(2, segments.get("instanceOne").size());
        assertEquals(1, segments.get("instanceTwo").size());
        assertEquals(1, segments.get("instanceThree").size());
    }

    @Test
    public void testNineSegmentForFiveInstances() {
        segments.put("instanceOne", of());
        segments.put("instanceTwo", of(0, 1, 2, 3));
        segments.put("instanceThree", of(4));
        segments.put("instanceFour", of(5, 6, 7));
        segments.put("instanceFive", of(8));
        testSubject.balance(eventProcessor).perform();
        assertEquals(1, segments.get("instanceOne").size());
        assertEquals(3, segments.get("instanceTwo").size());
        assertEquals(1, segments.get("instanceThree").size());
        assertEquals(3, segments.get("instanceFour").size());
        assertEquals(1, segments.get("instanceFive").size());
    }

    @Test
    public void testSameThreadPoolSize() {
        segments.put("instanceOne", of());
        segments.put("instanceTwo", of(0, 1, 2, 3));
        segments.put("instanceThree", of(4));
        segments.put("instanceFour", of(5, 6, 7));
        segments.put("instanceFive", of(8, 9));
        threadPoolSize.put("instanceOne", 5);
        threadPoolSize.put("instanceTwo", 5);
        threadPoolSize.put("instanceThree", 5);
        threadPoolSize.put("instanceFour", 5);
        threadPoolSize.put("instanceFive", 5);
        testSubject.balance(eventProcessor).perform();
        assertEquals(2, segments.get("instanceOne").size());
        assertEquals(2, segments.get("instanceTwo").size());
        assertEquals(2, segments.get("instanceThree").size());
        assertEquals(2, segments.get("instanceFour").size());
        assertEquals(2, segments.get("instanceFive").size());
    }

    @Test
    public void testDifferentThreadPoolSize() {
        segments.put("instanceOne", of());
        segments.put("instanceTwo", of(0, 1, 2, 3, 7));
        segments.put("instanceThree", of(4));
        segments.put("instanceFour", of(5, 6));
        segments.put("instanceFive", of(8, 9));
        threadPoolSize.put("instanceOne", 1);
        threadPoolSize.put("instanceTwo", 5);
        threadPoolSize.put("instanceThree", 1);
        threadPoolSize.put("instanceFour", 5);
        threadPoolSize.put("instanceFive", 6);
        testSubject.balance(eventProcessor).perform();
        assertEquals(1, segments.get("instanceOne").size());
        assertEquals(3, segments.get("instanceTwo").size());
        assertEquals(1, segments.get("instanceThree").size());
        assertEquals(2, segments.get("instanceFour").size());
        assertEquals(3, segments.get("instanceFive").size());
    }

    @Test
    public void testTwoEqualsNodes() {
        segments.put("instanceOne", of(0, 1, 2));
        segments.put("instanceTwo", of(3, 4, 5));
        segments.put("instanceThree", of());
        threadPoolSize.put("instanceOne", 4);
        threadPoolSize.put("instanceTwo", 4);
        threadPoolSize.put("instanceThree", 4);
        testSubject.balance(eventProcessor).perform();
        assertEquals(2, segments.get("instanceOne").size());
        assertEquals(2, segments.get("instanceTwo").size());
        assertEquals(2, segments.get("instanceThree").size());
    }

    private Collection<Integer> of(Integer... segments) {
        return new ArrayList<>(Arrays.asList(segments));
    }
}