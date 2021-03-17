/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.serializer.GsonMedia;
import org.junit.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Test class validating the {@link StreamingProcessor}.
 *
 * @author Sara Pellegrini
 */
public class StreamingProcessorTest {

    @Test
    public void testPrintOnCreatesFullyFledgedJson() {
        String expectedJson =
                "{\"name\":\"processor name\","
                        + "\"mode\":\"tracking\","
                        + "\"isStreaming\":true,"
                        + "\"fullName\":\"processor name@TokenStoreIdentifier\","
                        + "\"warnings\":[],"
                        + "\"tokenStoreIdentifier\":\"TokenStoreIdentifier\","
                        + "\"freeThreadInstances\":[\"clientIdOne\"],"
                        + "\"activeThreads\":2,"
                        + "\"canPause\":true,"
                        + "\"canPlay\":false,"
                        + "\"canSplit\":true,"
                        + "\"canMerge\":true,"
                        + "\"trackers\":["
                        + "{\"clientId\":\"clientIdOne\",\"segmentId\":0,\"caughtUp\":true,\"replaying\":false,\"tokenPosition\":0,\"errorState\":\"\",\"onePartOf\":2},"
                        + "{\"clientId\":\"clientIdTwo\",\"segmentId\":1,\"caughtUp\":true,\"replaying\":false,\"tokenPosition\":0,\"errorState\":\"\",\"onePartOf\":2}"
                        + "]}";

        EventProcessorInfo.SegmentStatus trackerInfo0 = EventProcessorInfo.SegmentStatus.newBuilder()
                                                                                        .setCaughtUp(true)
                                                                                        .setReplaying(false)
                                                                                        .setOnePartOf(2)
                                                                                        .setSegmentId(0)
                                                                                        .build();
        EventProcessorInfo processorInfo0 = EventProcessorInfo.newBuilder()
                                                              .setMode("Tracking")
                                                              .setTokenStoreIdentifier("TokenStoreIdentifier")
                                                              .setActiveThreads(1)
                                                              .setAvailableThreads(3)
                                                              .setRunning(true)
                                                              .addSegmentStatus(trackerInfo0)
                                                              .build();

        EventProcessorInfo.SegmentStatus trackerInfo1 = EventProcessorInfo.SegmentStatus.newBuilder()
                                                                                        .setCaughtUp(true)
                                                                                        .setReplaying(false)
                                                                                        .setOnePartOf(2)
                                                                                        .setSegmentId(1)
                                                                                        .build();
        EventProcessorInfo processorInfo1 = EventProcessorInfo.newBuilder()
                                                              .setMode("Tracking")
                                                              .setTokenStoreIdentifier("TokenStoreIdentifier")
                                                              .setActiveThreads(1)
                                                              .setAvailableThreads(0)
                                                              .setRunning(true)
                                                              .addSegmentStatus(trackerInfo1)
                                                              .build();
        List<ClientProcessor> testClientProcessors = asList(
                new FakeClientProcessor("clientIdOne", true, processorInfo0),
                new FakeClientProcessor("clientIdTwo", true, processorInfo1)
        );

        StreamingProcessor testSubject = new StreamingProcessor("processor name", "tracking", testClientProcessors);

        GsonMedia media = new GsonMedia();
        testSubject.printOn(media);

        assertEquals(expectedJson, media.toString());
    }

    @Test
    public void testPrintOnDisableCanMergeIfThereIsOnlyOneSegment() {
        String expectedJson =
                "{\"name\":\"processor name\","
                        + "\"mode\":\"tracking\","
                        + "\"isStreaming\":true,"
                        + "\"fullName\":\"processor name@TokenStoreIdentifier\","
                        + "\"warnings\":[],"
                        + "\"tokenStoreIdentifier\":\"TokenStoreIdentifier\","
                        + "\"freeThreadInstances\":[\"clientIdOne\"],"
                        + "\"activeThreads\":1,"
                        + "\"canPause\":true,"
                        + "\"canPlay\":false,"
                        + "\"canSplit\":true,"
                        + "\"canMerge\":false,"
                        + "\"trackers\":["
                        + "{\"clientId\":\"clientIdOne\",\"segmentId\":0,\"caughtUp\":true,\"replaying\":false,\"tokenPosition\":0,\"errorState\":\"\",\"onePartOf\":1}"
                        + "]}";

        EventProcessorInfo.SegmentStatus trackerInfo0 = EventProcessorInfo.SegmentStatus.newBuilder()
                                                                                        .setCaughtUp(true)
                                                                                        .setReplaying(false)
                                                                                        .setOnePartOf(1)
                                                                                        .setSegmentId(0)
                                                                                        .build();
        EventProcessorInfo processorInfo0 = EventProcessorInfo.newBuilder()
                                                              .setMode("Tracking")
                                                              .setTokenStoreIdentifier("TokenStoreIdentifier")
                                                              .setActiveThreads(1)
                                                              .setAvailableThreads(1)
                                                              .setRunning(true)
                                                              .addSegmentStatus(trackerInfo0)
                                                              .build();
        List<ClientProcessor> testClientProcessors =
                Collections.singletonList(new FakeClientProcessor("clientIdOne", true, processorInfo0));

        StreamingProcessor testSubject = new StreamingProcessor("processor name", "tracking", testClientProcessors);

        GsonMedia media = new GsonMedia();
        testSubject.printOn(media);

        assertEquals(expectedJson, media.toString());
    }

    @Test
    public void testPrintOnEnableCanMergeWhenOnlyOneSegmentOfMultipleIsClaimed() throws IOException {

        EventProcessorInfo.SegmentStatus trackerInfo0 = EventProcessorInfo.SegmentStatus.newBuilder()
                                                                                        .setCaughtUp(true)
                                                                                        .setReplaying(false)
                                                                                        .setOnePartOf(2)
                                                                                        .setSegmentId(0)
                                                                                        .build();
        EventProcessorInfo processorInfo0 = EventProcessorInfo.newBuilder()
                                                              .setMode("Tracking")
                                                              .setTokenStoreIdentifier("TokenStoreIdentifier")
                                                              .setActiveThreads(1)
                                                              .setAvailableThreads(1)
                                                              .setRunning(true)
                                                              .addSegmentStatus(trackerInfo0)
                                                              .build();
        List<ClientProcessor> testClientProcessors =
                Collections.singletonList(new FakeClientProcessor("clientIdOne", true, processorInfo0));

        StreamingProcessor testSubject = new StreamingProcessor("processor name", "tracking", testClientProcessors);

        GsonMedia media = new GsonMedia();
        testSubject.printOn(media);

        JsonNode actual = new ObjectMapper().reader().readTree(media.toString());
        assertTrue(actual.at("/canMerge").booleanValue());
        assertEquals("Not all segments claimed", actual.at("/warnings/0/message").textValue());
    }
}
