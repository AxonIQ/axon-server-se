/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin;

import org.junit.Test;

import static io.axoniq.axonserver.admin.Result.ACK;
import static io.axoniq.axonserver.admin.Result.FAILURE;
import static io.axoniq.axonserver.admin.Result.SUCCESS;
import static org.junit.Assert.assertEquals;

public class ResultTest {

    @Test
    public void and() {
        assertEquals(SUCCESS, SUCCESS.and(SUCCESS));
        assertEquals(ACK, SUCCESS.and(ACK));
        assertEquals(ACK, ACK.and(SUCCESS));
        assertEquals(FAILURE, ACK.and(FAILURE));
        assertEquals(FAILURE, SUCCESS.and(FAILURE));
        assertEquals(FAILURE, FAILURE.and(FAILURE));
    }
}