/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class TransformationProgressUpdate implements TransformationProgress {

    private final long lastTokenProcessed;

    public TransformationProgressUpdate(long lastTokenProcessed) {
        this.lastTokenProcessed = lastTokenProcessed;
    }

    public long lastTokenProcessed() {
        return lastTokenProcessed;
    }

    @Override
    public String toString() {
        return "TransformationProgressUpdate{" +
                "lastTokenProcessed=" + lastTokenProcessed +
                '}';
    }
}