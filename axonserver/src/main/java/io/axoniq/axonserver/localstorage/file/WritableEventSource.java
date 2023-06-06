/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;

import static io.axoniq.axonserver.localstorage.file.AbstractFileStorageTier.TRANSACTION_VERSION;
import static io.axoniq.axonserver.localstorage.file.FileEventStorageEngine.MAX_EVENTS_PER_BLOCK;

/**
 * @author Marc Gathier
 */
public class WritableEventSource extends ByteBufferEventSource {

    private static final Logger logger = LoggerFactory.getLogger(WritableEventSource.class);
    private final int limit;
    private final int capacity;

    public WritableEventSource(String file, ByteBuffer buffer, long segment, int version,
                               EventTransformer eventTransformer, StorageProperties storageProperties) {
        super(file, buffer, segment, version, flags -> eventTransformer, storageProperties);
        limit = buffer.limit();
        capacity = buffer.capacity();
    }


    public int limit() {
        return limit;
    }

    public int capacity() {
        return capacity;
    }

    public void force() {
        duplicatesCount.incrementAndGet();
        try {
            checkClosed();
            ((MappedByteBuffer) buffer).force();
        } catch (Exception ex) {
            logger.debug("Force failed", ex);
        } finally {
            duplicatesCount.decrementAndGet();
        }
    }

    public int getInt(int position) {
        return buffer.getInt(position);
    }


    public void putInt(int position, int value) {
        buffer.putInt(position, value);
    }

    public void write(List<ProcessedEvent> eventList, int position, long sequence,
                      EventPositionProcessor eventPositionProcessor) {
        duplicatesCount.incrementAndGet();
        try {
            checkClosed();
            ByteBuffer writeBuffer = buffer.duplicate();
            writeBuffer.position(position);
            int count = eventList.size();
            int from = 0;
            int to = Math.min(count, from + MAX_EVENTS_PER_BLOCK);
            int firstSize = writeBlock(writeBuffer, eventList, 0, to, sequence, eventPositionProcessor);
            while (to < count) {
                from = to;
                to = Math.min(count, from + MAX_EVENTS_PER_BLOCK);
                int positionBefore = writeBuffer.position();
                int blockSize = writeBlock(writeBuffer, eventList, from, to, sequence + from, eventPositionProcessor);
                int positionAfter = writeBuffer.position();
                writeBuffer.putInt(positionBefore, blockSize);
                writeBuffer.position(positionAfter);
            }
            writeBuffer.putInt(position, firstSize);
        } finally {
            duplicatesCount.decrementAndGet();
        }
    }

    private int writeBlock(ByteBuffer writeBuffer, List<ProcessedEvent> eventList, int from, int to,
                           long token, EventPositionProcessor eventPositionProcessor) {
        writeBuffer.putInt(0);
        writeBuffer.put(TRANSACTION_VERSION);
        writeBuffer.putShort((short) (to - from));
        Checksum checksum = new Checksum();
        int eventsPosition = writeBuffer.position();
        int eventsSize = 0;
        for (int i = from; i < to; i++) {
            ProcessedEvent event = eventList.get(i);
            int position = writeBuffer.position();
            writeBuffer.putInt(event.getSerializedSize());
            writeBuffer.put(event.toByteArray());
            eventPositionProcessor.process(event, token, position);
            eventsSize += event.getSerializedSize() + 4;
            token++;
        }

        writeBuffer.putInt(checksum.update(writeBuffer, eventsPosition, writeBuffer.position() - eventsPosition).get());
        return eventsSize;
    }
}
