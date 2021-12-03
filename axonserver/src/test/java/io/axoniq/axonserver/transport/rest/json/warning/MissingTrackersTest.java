/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json.warning;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorSegment;
import io.axoniq.axonserver.admin.eventprocessor.api.FakeEventProcessorSegment;
import org.junit.*;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Unit tests for {@link MissingTrackers}
 *
 * @author Sara Pellegrini
 */
public class MissingTrackersTest {

    @Test
    public void testActive() {
        List<EventProcessorSegment> eventTrackerInfoList = asList(new FakeEventProcessorSegment(1, 2),
                                                                  new FakeEventProcessorSegment(2, 4));

        MissingTrackers warning = new MissingTrackers(eventTrackerInfoList);
        Assert.assertTrue(warning.active());
    }

    @Test
    public void testNotActive() {
        List<EventProcessorSegment> eventTrackerInfoList = asList(new FakeEventProcessorSegment(1, 2),
                                                                  new FakeEventProcessorSegment(2, 2));

        MissingTrackers warning = new MissingTrackers(eventTrackerInfoList);
        Assert.assertFalse(warning.active());
    }

    @Test
    public void testActiveWithDuplicatesSegments() {
        List<EventProcessorSegment> eventTrackerInfoList = asList(new FakeEventProcessorSegment(1, 2),
                                                                  new FakeEventProcessorSegment(1, 4));

        MissingTrackers warning = new MissingTrackers(eventTrackerInfoList);
        Assert.assertTrue(warning.active());
    }

    @Test
    public void testNotActiveWithDuplicatesSegments() {
        List<EventProcessorSegment> eventTrackerInfoList = asList(new FakeEventProcessorSegment(1, 2),
                                                                  new FakeEventProcessorSegment(2, 2),
                                                                  new FakeEventProcessorSegment(1, 2),
                                                                  new FakeEventProcessorSegment(2, 2)
        );

        Warning warning = new MissingTrackers(eventTrackerInfoList);
        Assert.assertFalse(warning.active());
    }
}
