/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

/**
 * @author Marc Gathier
 */
public class WritableEventSource extends ByteBufferEventSource {
    private static final Logger logger = LoggerFactory.getLogger(WritableEventSource.class);

    public WritableEventSource(String file, ByteBuffer buffer, EventTransformer eventTransformer, boolean cleanerHack) {
        super(file, buffer, eventTransformer, cleanerHack);
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

    public void clearTo(int endPosition) {
        int currentPosition = getBuffer().position();
        byte[] emptyBytes = new byte[1024*8];
        Arrays.fill(emptyBytes, (byte)0);
        for( int pos = currentPosition ; pos < endPosition; pos += emptyBytes.length) {
            getBuffer().put(emptyBytes, 0, Math.min(emptyBytes.length, endPosition - pos));
        }

    }
}
