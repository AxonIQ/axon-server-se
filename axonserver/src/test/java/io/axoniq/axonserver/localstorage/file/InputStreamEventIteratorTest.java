/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.test.TestUtils;
import org.junit.*;

import java.io.File;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 * @since 4.5.7
 */
public class InputStreamEventIteratorTest {
    private InputStreamEventIterator testSubject;

    @Before
    public void setUp() throws Exception {

        File file = new File(TestUtils
                .fixPathOnWindows(InputStreamEventStore.class
                                          .getResource(
                                                  "/data/default/00000000000000000000.events")
                                          .getFile()));
        InputStreamEventSource eventSource = new InputStreamEventSource(file, new DefaultEventTransformerFactory());
        testSubject = new InputStreamEventIterator(eventSource, 0, 0);
    }

    @Test
    public void iterateClosedFile() {
        testSubject.close();
        try {
            testSubject.hasNext();
            fail("Reading from closed iterator must fail");
        } catch (IllegalStateException expected) {
            // expected illegal state exception
        }
    }
}