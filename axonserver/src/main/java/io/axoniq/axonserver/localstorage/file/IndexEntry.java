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
 * Contains information on a new entry to be added to the index in the current segment.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class IndexEntry {

    private final long sequenceNumber;
    private final int position;
    private final long token;

    /**
     * @param sequenceNumber the sequence number of the event for the aggregate
     * @param position       the position of the event in the event file
     * @param token          the global token of the event
     */
    public IndexEntry(long sequenceNumber, int position, long token) {
        this.sequenceNumber = sequenceNumber;
        this.position = position;
        this.token = token;
    }

    /**
     * Returns the sequence number.
     *
     * @return the sequence number
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Returns the position.
     *
     * @return the position
     */
    public int getPosition() {
        return position;
    }

    /**
     * Returns the token.
     *
     * @return the token
     */
    public long getToken() {
        return token;
    }
}
