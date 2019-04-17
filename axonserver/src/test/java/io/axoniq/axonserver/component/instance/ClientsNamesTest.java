/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.instance;

import org.junit.*;

import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 16/05/2018.
 * sara.pellegrini@gmail.com
 */
public class ClientsNamesTest {

    @Test
    public void iterator() {
        Clients clients = () -> asList((Client) new FakeClient("A", false),
                                       new FakeClient("B", false)).iterator();
        Iterator<String> iterator = new ClientsNames(clients).iterator();
        assertTrue(iterator.hasNext());
        assertEquals("A", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("B", iterator.next());
        assertFalse(iterator.hasNext());
    }
}