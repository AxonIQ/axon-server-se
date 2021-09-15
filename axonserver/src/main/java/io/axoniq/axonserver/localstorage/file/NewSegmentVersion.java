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
 * @since
 */
public class NewSegmentVersion implements TransformationProgress {

    private final long segment;
    private final int version;
    private final long nextToken;

    public NewSegmentVersion(long segment, int version, long nextToken) {
        this.segment = segment;
        this.version = version;
        this.nextToken = nextToken;
    }

    public long segment() {
        return segment;
    }

    public int version() {
        return version;
    }

    public long nextToken() {
        return nextToken;
    }

    @Override
    public String toString() {
        return "NewSegmentVersion{" +
                "segment=" + segment +
                ", version=" + version +
                ", nextToken=" + nextToken +
                '}';
    }
}
