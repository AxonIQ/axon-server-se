/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
public class SegmentIndexEntries {

    private final long segment;
    private final IndexEntries indexEntries;

    public SegmentIndexEntries(long segment, IndexEntries indexEntries) {
        this.segment = segment;
        this.indexEntries = indexEntries;
    }

    public long segment() {
        return segment;
    }

    public IndexEntries indexEntries() {
        return indexEntries;
    }
}
