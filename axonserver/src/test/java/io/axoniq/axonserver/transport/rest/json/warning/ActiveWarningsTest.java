/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json.warning;

import org.junit.*;

import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link ActiveWarnings}
 *
 * @author Sara Pellegrini
 */
public class ActiveWarningsTest {

    @Test
    public void testOne() {
        Warning activeWarning = new FakeWarning(true, "Active warning");
        List<Warning> fakes = asList(new FakeWarning(false, "Inactive warning"),
                                     activeWarning,
                                     new FakeWarning(false, "Other inactive warning")
        );

        Iterator<Warning> iterator = new ActiveWarnings(fakes).iterator();
        Assert.assertTrue(iterator.hasNext());
        assertEquals(activeWarning, iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testNone() {
        List<Warning> fakes = asList(new FakeWarning(false, "Inactive warning"),
                                     new FakeWarning(false, "Other inactive warning")
        );

        Iterator<Warning> iterator = new ActiveWarnings(fakes).iterator();
        Assert.assertFalse(iterator.hasNext());
    }
}