/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin;

public enum Result {
    SUCCESS(0),
    ACK(1),
    FAILURE(2);

    private final int sequence;

    Result(int sequence) {
        this.sequence = sequence;
    }

    public Result and(Result result) {
        return result.sequence > this.sequence ? result : this;
    }
}
