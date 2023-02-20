/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.Event;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.axoniq.axonserver.localstorage.file.AbstractFileStorageTier.TRANSACTION_VERSION;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class StreamSegmentWriter implements SegmentWriter {
    private final DataOutputStream dataOutputStream;
    private final Map<String, List<IndexEntry>> indexEntriesMap = new HashMap<>();
    private int pos = 5;
    private long token;

    public StreamSegmentWriter(File file, long segment, int flags) throws IOException {
        dataOutputStream = new DataOutputStream(new FileOutputStream(file));
        dataOutputStream.write(AbstractFileStorageTier.EVENT_FORMAT_VERSION);
        dataOutputStream.writeInt(flags);
        token = segment-1;
    }

    @Override
    public void close() throws IOException {
        dataOutputStream.close();
    }

    @Override
    public long lastToken() {
        return token;
    }

    @Override
    public void writeEndOfFile() throws IOException {
        dataOutputStream.writeInt(-1);
    }

    @Override
    public void write(List<Event> events) throws IOException {
        int eventPosition = pos + 7;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream eventsBlock = new DataOutputStream(bytes);
        for (Event updatedEvent : events) {
            token++;
            int size = updatedEvent.getSerializedSize();
            eventsBlock.writeInt(size);
            eventsBlock.write(updatedEvent.toByteArray());
            if (!updatedEvent.getAggregateType().isEmpty()) {
                indexEntriesMap.computeIfAbsent(updatedEvent.getAggregateIdentifier(), id -> new ArrayList<>())
                               .add(new IndexEntry(updatedEvent.getAggregateSequenceNumber(),
                                                   eventPosition,
                                                   token));
            }
            eventPosition += size + 4;
        }

        byte[] eventBytes = bytes.toByteArray();
        dataOutputStream.writeInt(eventBytes.length);
        dataOutputStream.write(TRANSACTION_VERSION);
        dataOutputStream.writeShort(events.size());
        dataOutputStream.write(eventBytes);
        Checksum checksum = new Checksum();
        pos += eventBytes.length + 4 + 7;
        dataOutputStream.writeInt(checksum.update(eventBytes).get());
    }

    @Override
    public Map<String, List<IndexEntry>> indexEntries() {
        return indexEntriesMap;
    }
}
