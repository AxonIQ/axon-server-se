/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorInstance;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorSegment;
import io.axoniq.axonserver.admin.eventprocessor.api.FakeEvenProcessorInstance;
import io.axoniq.axonserver.admin.eventprocessor.api.FakeEventProcessor;
import io.axoniq.axonserver.admin.eventprocessor.api.FakeEventProcessorSegment;
import io.axoniq.axonserver.serializer.GsonMedia;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
                        + "\"context\":\"default\","
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

        EventProcessorSegment trackerInfo0 = new FakeEventProcessorSegment(0,
                                                                           2,
                                                                           false,
                                                                           true,
                                                                           "clientIdOne");

        EventProcessorInstance instance0 = new FakeEvenProcessorInstance("clientIdOne",
                                                                         true,
                                                                         4,
                                                                         singletonList(trackerInfo0));

        EventProcessorSegment trackerInfo1 = new FakeEventProcessorSegment(1,
                                                                           2,
                                                                           false,
                                                                           true,
                                                                           "clientIdTwo");
        EventProcessorInstance instance1 = new FakeEvenProcessorInstance("clientIdTwo",
                                                                         true,
                                                                         1,
                                                                         singletonList(trackerInfo1));

        EventProcessor eventProcessor = new FakeEventProcessor("processor name",
                                                               "TokenStoreIdentifier",
                                                               true,
                                                               "tracking",
                                                               asList(instance0, instance1)
        );
        StreamingProcessor testSubject = new StreamingProcessor(eventProcessor);

        GsonMedia media = new GsonMedia();
        testSubject.printOn(media);

        assertEquals(expectedJson, media.toString());
    }

    @Test
    public void testPrintOnDisableCanMergeIfThereIsOnlyOneSegment() {
        String expectedJson =
                "{\"name\":\"processor name\","
                        + "\"context\":\"default\","
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


        EventProcessorSegment trackerInfo0 = new FakeEventProcessorSegment(0,
                                                                           1,
                                                                           false,
                                                                           true,
                                                                           "clientIdOne");

        EventProcessorInstance instance0 = new FakeEvenProcessorInstance("clientIdOne",
                                                                         true,
                                                                         4,
                                                                         singletonList(trackerInfo0));

        EventProcessor eventProcessor = new FakeEventProcessor("processor name",
                                                               "TokenStoreIdentifier",
                                                               true,
                                                               "tracking",
                                                               singletonList(instance0)
        );
        StreamingProcessor testSubject = new StreamingProcessor(eventProcessor);

        GsonMedia media = new GsonMedia();
        testSubject.printOn(media);

        assertEquals(expectedJson, media.toString());
    }

    @Test
    public void testPrintOnEnableCanMergeWhenOnlyOneSegmentOfMultipleIsClaimed()
            throws JsonProcessingException {
        EventProcessorSegment trackerInfo0 = new FakeEventProcessorSegment(0,
                                                                           2,
                                                                           false,
                                                                           true,
                                                                           "clientIdOne");

        EventProcessorInstance instance0 = new FakeEvenProcessorInstance("clientIdOne",
                                                                         true,
                                                                         2,
                                                                         singletonList(trackerInfo0));

        EventProcessor eventProcessor = new FakeEventProcessor("processor name",
                                                               "TokenStoreIdentifier",
                                                               true,
                                                               "tracking",
                                                               singletonList(instance0)
        );
        StreamingProcessor testSubject = new StreamingProcessor(eventProcessor);
        GsonMedia media = new GsonMedia();
        testSubject.printOn(media);

        JsonNode actual = new ObjectMapper().reader().readTree(media.toString());
        assertTrue(actual.at("/canMerge").booleanValue());
        assertEquals("Not all segments claimed", actual.at("/warnings/0/message").textValue());
    }
}
