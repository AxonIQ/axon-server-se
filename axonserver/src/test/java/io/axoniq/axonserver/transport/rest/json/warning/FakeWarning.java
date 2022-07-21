/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json.warning;

/**
 * Fake implementation of {@link Warning} for test purpose.
 */
public class FakeWarning implements Warning {

    private final boolean active;

    private final String message;

    public FakeWarning(boolean active, String message) {
        this.active = active;
        this.message = message;
    }

    @Override
    public boolean active() {
        return active;
    }

    @Override
    public String message() {
        return message;
    }
}
