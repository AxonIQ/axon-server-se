/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

/**
 * @author Marc Gathier
 */
public class SegmentAndPosition {

    private final long segment;
    private final int position;

    public SegmentAndPosition(long segment, int position) {
        this.segment = segment;
        this.position = position;
    }

    public long getSegment() {
        return segment;
    }

    public int getPosition() {
        return position;
    }
}
