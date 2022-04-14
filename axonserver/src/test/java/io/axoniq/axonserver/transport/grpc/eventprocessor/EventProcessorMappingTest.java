/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc.eventprocessor;


import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorInstance;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorSegment;
import io.axoniq.axonserver.admin.eventprocessor.api.FakeEvenProcessorInstance;
import io.axoniq.axonserver.admin.eventprocessor.api.FakeEventProcessor;
import io.axoniq.axonserver.admin.eventprocessor.api.FakeEventProcessorSegment;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link EventProcessorMapping}
 *
 * @author Sara Pellegrini
 */
public class EventProcessorMappingTest {

    @Test
    public void testMapping() {
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

        String expectedResult = "identifier {\n"
                + "  processor_name: \"processor name\"\n"
                + "  token_store_identifier: \"TokenStoreIdentifier\"\n"
                + "}\n"
                + "mode: \"tracking\"\n"
                + "isStreaming: true\n"
                + "client_instance {\n"
                + "  client_id: \"clientIdOne\"\n"
                + "  isRunning: true\n"
                + "  max_capacity: 4\n"
                + "  claimed_segment {\n"
                + "    one_part_of: 2\n"
                + "    claimed_by: \"clientIdOne\"\n"
                + "    is_caughtUp: true\n"
                + "  }\n"
                + "}\n"
                + "client_instance {\n"
                + "  client_id: \"clientIdTwo\"\n"
                + "  isRunning: true\n"
                + "  max_capacity: 1\n"
                + "  claimed_segment {\n"
                + "    id: 1\n"
                + "    one_part_of: 2\n"
                + "    claimed_by: \"clientIdTwo\"\n"
                + "    is_caughtUp: true\n"
                + "  }\n"
                + "}\n";

        EventProcessorMapping testSubject = new EventProcessorMapping();
        assertEquals(expectedResult, testSubject.apply(eventProcessor).toString());
    }
}