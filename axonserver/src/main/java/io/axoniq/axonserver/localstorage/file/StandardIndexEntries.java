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
import java.util.List;
import java.util.Optional;

/**
 * @author Marc Gathier
 */
public class StandardIndexEntries implements IndexEntries {

    private final List<IndexEntry> entries;
    private final long firstSequenceNumber;

    public StandardIndexEntries(long firstSequenceNumber) {
        this(firstSequenceNumber, new ArrayList<>());
    }

    public StandardIndexEntries(long firstSequenceNumber, List<IndexEntry> entries) {

        this.entries = entries;
        this.firstSequenceNumber = firstSequenceNumber;
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public IndexEntries range(long minSequenceNumber, long maxSequenceNumber) {
        List<IndexEntry> reducedEntries = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            if (firstSequenceNumber + i >= minSequenceNumber && firstSequenceNumber + i < maxSequenceNumber) {
                reducedEntries.add(entries.get(i));
            }
        }
        return new StandardIndexEntries(Math.max(minSequenceNumber, firstSequenceNumber), reducedEntries);
    }

    @Override
    public List<IndexEntry> positions() {
        return entries;
    }

    @Override
    public int last() {
        return lastIndexEntry().map(IndexEntry::getPosition).orElse(-1);
    }

    @Override
    public Optional<IndexEntry> lastIndexEntry() {
        if (entries.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(entries.get(entries.size() - 1));
    }

    @Override
    public long lastSequenceNumber() {
        if (entries.isEmpty()) {
            return -1;
        }
        return firstSequenceNumber + entries.size() - 1;
    }

    @Override
    public void add(IndexEntry indexEntry) {
        entries.add(indexEntry);
    }

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
