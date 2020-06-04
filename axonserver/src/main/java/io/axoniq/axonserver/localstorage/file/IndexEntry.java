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
public class IndexEntry {

    private final long sequenceNumber;
    private final int position;
    private final long token;

    public IndexEntry(long sequenceNumber, int position) {
        this(sequenceNumber, position, -1);
    }

    public IndexEntry(long sequenceNumber, int position, long token) {
        this.sequenceNumber = sequenceNumber;
        this.position = position;
        this.token = token;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public int getPosition() {
        return position;
    }

    public long getToken() {
        return token;
    }
}
