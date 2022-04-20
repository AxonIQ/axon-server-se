/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorSegment;
import io.axoniq.axonserver.admin.eventprocessor.api.FakeEventProcessorSegment;
import io.axoniq.axonserver.serializer.GsonMedia;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Test class validating the {@link StreamingProcessorSegment}.
 *
 * @author Sara Pellegrini
 */
public class StreamingProcessorSegmentTest {

    @Test
    public void printOn() {
        GsonMedia gsonMedia = new GsonMedia();
        EventProcessorSegment eventTrackerInfo = new FakeEventProcessorSegment(1,
                                                                               2,
                                                                               false,
                                                                               true,
                                                                               "myClient");
        StreamingProcessorSegment tracker = new StreamingProcessorSegment(eventTrackerInfo);
        tracker.printOn(gsonMedia);
        assertEquals("{\"clientId\":\"myClient\","
                             + "\"segmentId\":1,"
                             + "\"caughtUp\":true,"
                             + "\"replaying\":false,"
                             + "\"tokenPosition\":0,"
                             + "\"errorState\":\"\","
                             + "\"onePartOf\":2}",
                     gsonMedia.toString());
    }
}
