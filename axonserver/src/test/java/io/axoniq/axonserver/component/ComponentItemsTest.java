/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component;

import io.axoniq.axonserver.topology.Topology;
import org.junit.*;

import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ComponentItemsTest {

    @Test
    public void testSomeMatch(){
        FakeComponentItem matching = new FakeComponentItem(true);
        List<FakeComponentItem> fakes = asList(new FakeComponentItem(false), matching, new FakeComponentItem(false));
        ComponentItems<FakeComponentItem> component = new ComponentItems<>("component", Topology.DEFAULT_CONTEXT, fakes);
        Iterator<FakeComponentItem> iterator = component.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(matching, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNoMatch(){
        List<FakeComponentItem> fakes = asList(new FakeComponentItem(false), new FakeComponentItem(false));
        ComponentItems<FakeComponentItem> component = new ComponentItems<>("component", Topology.DEFAULT_CONTEXT, fakes);
        Iterator<FakeComponentItem> iterator = component.iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testAllMatch(){
        FakeComponentItem matching = new FakeComponentItem(true);
        List<FakeComponentItem> fakes = asList(matching, matching);
        ComponentItems<FakeComponentItem> component = new ComponentItems<>("component", Topology.DEFAULT_CONTEXT, fakes);
        Iterator<FakeComponentItem> iterator = component.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(matching, iterator.next());
        assertEquals(matching, iterator.next());
        assertFalse(iterator.hasNext());
    }

}