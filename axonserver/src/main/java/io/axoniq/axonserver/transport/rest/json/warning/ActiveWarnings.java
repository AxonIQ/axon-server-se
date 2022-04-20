/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json.warning;

import java.util.Iterator;

import static java.util.stream.StreamSupport.stream;

/**
 * Iterable of active warnings.
 */
public class ActiveWarnings implements Iterable<Warning> {

    private final Iterable<Warning> warnings;

    public ActiveWarnings(Iterable<Warning> warnings) {
        this.warnings = warnings;
    }

    @Override
    public Iterator<Warning> iterator() {
        return stream(warnings.spliterator(), false)
                .filter(Warning::active)
                .iterator();
    }
}
