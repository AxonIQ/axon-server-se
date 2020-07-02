/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import java.util.List;

/**
 * Describes the index entries for a single segment.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public interface IndexEntries {

    /**
     * Checks if there are entries in this instance.
     *
     * @return true if there are no entries in this instance
     */
    boolean isEmpty();

    /**
     * Returns the number of entries.
     *
     * @return number of entries
     */
    int size();

    /**
     * Returns {@link IndexEntries} object with sequence numbers in range between {@code minSequenceNumber} and
     * {@code maxSequenceNumber}.
     *
     * @param minSequenceNumber the lowest sequence number to include
     * @param maxSequenceNumber maximum sequence number (excluded)
     * @param snapshot          flag to indicate if the index is for snapshots.
     * @return object containing the positions within the given range
     */
    IndexEntries range(long minSequenceNumber, long maxSequenceNumber, boolean snapshot);

    /**
     * Returns the positions of events within this segment.
     *
     * @return the positions of events within this segment
     */
    List<Integer> positions();

    /**
     * Returns the last position of an event.
     *
     * @return the last position
     */
    int last();

    /**
     * Returns the last sequence number.
     *
     * @return the last sequence number
     */
    long lastSequenceNumber();

    /**
     * Adds a new index entry.
     *
     * @param indexEntry the index entry to add
     */
    void add(IndexEntry indexEntry);

    /**
     * Returns the first sequence number.
     *
     * @return the first sequence number
     */
    long firstSequenceNumber();

    /**
     * Adds a number of positions to the index entries for an aggregate.
     *
     * @param entries list of positions
     */
    void addAll(List<IndexEntry> entries);
}
