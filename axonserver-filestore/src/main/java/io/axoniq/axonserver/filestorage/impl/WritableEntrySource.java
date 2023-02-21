/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.filestorage.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class WritableEntrySource extends ByteBufferEntrySource {
    private static final Logger logger = LoggerFactory.getLogger(WritableEntrySource.class);
    public WritableEntrySource(ByteBuffer buffer, boolean cleanerHackNeeded, long segment) {
        super(buffer, cleanerHackNeeded, segment);
    }

    private MappedByteBuffer mappedByteBuffer() {
        return (MappedByteBuffer)getBuffer();
    }

    public int limit() {
        return getBuffer().limit();
    }

    public int capacity() {
        return getBuffer().capacity();
    }

    public void force() {
        try {
            mappedByteBuffer().force();
        } catch( Exception ex) {
            logger.debug("Force failed", ex);
        }
    }

    public int getInt(int position) {
        return getBuffer().getInt(position);
    }


    public void putInt(int position, int value) {
        getBuffer().putInt(position, value);
    }

    public void position(int startPosition) {
        getBuffer().position(startPosition);
    }

    public void clearFromPosition() {
        byte[] bytes = new byte[10240];
        Arrays.fill(bytes, (byte) 0);
        ByteBuffer buffer = getBuffer();
        int position = buffer.position();
        while( buffer.remaining() > 0) {
            buffer.put(bytes, 0, Math.min(bytes.length, buffer.remaining()));
        }
        buffer.position(position);
    }
}
