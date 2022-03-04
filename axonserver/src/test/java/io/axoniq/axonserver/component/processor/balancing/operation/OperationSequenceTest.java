/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing.operation;

import org.junit.*;

import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 20/08/2018. sara.pellegrini@gmail.com
 */
public class OperationSequenceTest {

    @Test
    public void perform() {
        List<String> operations = new LinkedList<>();
        OperationSequence testSubject = new OperationSequence(asList(() -> operations.add("A"),
                                                                     () -> operations.add("B"),
                                                                     () -> operations.add("C")));
        testSubject.perform();
        assertEquals(asList("A", "B", "C"), operations);
    }
}