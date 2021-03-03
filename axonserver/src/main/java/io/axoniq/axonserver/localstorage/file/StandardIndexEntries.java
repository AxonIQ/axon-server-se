/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of the {@link IndexEntries} used by the {@link StandardIndexManager}.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class StandardIndexEntries implements IndexEntries {

    private final AppendOnlyList<Integer> entries;
    private final long firstSequenceNumber;

    /**
     * Initializes the object with an empty list of entries and given {@code firstSequenceNumber}.
     * @param firstSequenceNumber first sequence number
     */
    public StandardIndexEntries(long firstSequenceNumber) {
        this(firstSequenceNumber, Collections.emptyList());
    }

    /**
     * Initializes the object with given entries and {@code firstSequenceNumber}.
     * @param firstSequenceNumber first sequence number
     * @param entries the positions of the aggregate
     */
    public StandardIndexEntries(long firstSequenceNumber, List<Integer> entries) {
        this.entries = new AppendOnlyList<>(entries);
        this.firstSequenceNumber = firstSequenceNumber;
    }

    /**
     * @return true if no entries in this object
     */
    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * @return number of positions
     */
    @Override
    public int size() {
        return entries.size();
    }

    /**
     * Returns an {@link IndexEntries} object with positions within the given sequence range.
     * For snapshots we cannot use the range here as the sequence numbers of snapshots are not sequential
     * need to check the sequence number for the entries when we retrieve them from the event store
     * @param minSequenceNumber the lowest sequence number to include
     * @param maxSequenceNumber maximum sequence number (excluded)
     * @param snapshot          flag to indicate if the index is for snapshots.
     * @return an {@link IndexEntries} object with positions within the given sequence range
     */
    @Override
    public IndexEntries range(long minSequenceNumber, long maxSequenceNumber, boolean snapshot) {
        if (snapshot) {
            return this;
        }
        List<Integer> reducedEntries = new ArrayList<>();
        long i = firstSequenceNumber;
        for (Integer entry : entries) {
            if (i >= minSequenceNumber && i < maxSequenceNumber) {
                reducedEntries.add(entry);
            }
            i++;
        }
        return new StandardIndexEntries(Math.max(minSequenceNumber, firstSequenceNumber), reducedEntries);
    }

    /**
     * @return list of positions
     */
    @Override
    public List<Integer> positions() {
        return entries;
    }

    /**
     * @return position of the last entry
     */
    @Override
    public int last() {
        if (isEmpty()) {
            return -1;
        }
        return entries.last();
    }

    /**
     * @return the last sequence number
     */
    @Override
    public long lastSequenceNumber() {
        if (entries.isEmpty()) {
            return -1;
        }
        return firstSequenceNumber + entries.size() - 1;
    }

    /**
     * Adds a new entry
     *
     * @param indexEntry the index entry to add
     */
    @Override
    public void add(IndexEntry indexEntry) {
        add(indexEntry.getPosition());
    }

    public void add(Integer position) {
        entries.add(position);
    }

    @Override
    public void addAll(List<IndexEntry> newEntries) {
        List<Integer> positions = new ArrayList<>(newEntries.size());
        newEntries.forEach(e -> positions.add(e.getPosition()));
        entries.addAll(positions);
    }

    /**
     * @return the first sequence number
     */
    @Override
    public long firstSequenceNumber() {
        if (entries.isEmpty()) {
            return -1;
        }
        return firstSequenceNumber;
    }

    @Override
    public String toString() {
        return "size: " + entries.size() + ", first: " + firstSequenceNumber() + ", last: " + lastSequenceNumber();
    }
}
