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
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 20/08/2018. sara.pellegrini@gmail.com
 */
public class OperationSequenceTest {

    private static final String INSTRUCTION_ID = UUID.randomUUID().toString();

    @Test
    public void perform() {
        List<String> operations = new LinkedList<>();
        OperationSequence testSubject = new OperationSequence(asList((String instructionId) -> operations.add("A"),
                                                                     (String instructionId) -> operations.add("B"),
                                                                     (String instructionId) -> operations.add("C")));
        testSubject.perform(INSTRUCTION_ID);
        assertEquals(asList("A", "B", "C"), operations);
    }
}