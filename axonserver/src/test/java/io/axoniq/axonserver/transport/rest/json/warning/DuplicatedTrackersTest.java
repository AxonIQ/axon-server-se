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
 * Unit tests for {@link DuplicatedTrackers}.
 *
 * @author Sara Pellegrini
 */
public class DuplicatedTrackersTest {

    @Test
    public void testActive() {
        List<EventProcessorSegment> eventTrackerInfoList = asList(new FakeEventProcessorSegment(1),
                                                                  new FakeEventProcessorSegment(1));
        Warning warning = new DuplicatedTrackers(eventTrackerInfoList);
        Assert.assertTrue(warning.active());
    }

    @Test
    public void testNotActive() {
        List<EventProcessorSegment> eventTrackerInfoList = asList(new FakeEventProcessorSegment(2),
                                                                  new FakeEventProcessorSegment(3));
        Warning warning = new DuplicatedTrackers(eventTrackerInfoList);
        Assert.assertFalse(warning.active());
    }
}
