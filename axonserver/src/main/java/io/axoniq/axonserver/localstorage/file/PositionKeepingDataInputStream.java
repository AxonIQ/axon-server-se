/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Marc Gathier
 */
public class PositionKeepingDataInputStream {

    private int position = 0;
    private RandomAccessFile reader;

    public PositionKeepingDataInputStream(File file) throws FileNotFoundException {
        reader = new RandomAccessFile(file, "r");
    }

    public byte readByte() throws IOException {
        byte b = reader.readByte();
        position++;
        return b;
    }

    public int readInt() throws IOException {
        int i = reader.readInt();
        position += 4;
        return i;
    }

    public void position(int newPosition) throws IOException {
        reader.seek(newPosition);
        this.position = newPosition;
    }

    public byte[] readEvent() throws IOException {
        int size = readInt();
        return readBytes(size);
    }

    private byte[] readBytes(int size) throws IOException {
        try {
            byte[] bytes = new byte[size];
            int total = reader.read(bytes);
            while( total < size ) {
                total += reader.read(bytes, total, size-total);
            }
            position  += size;
            return bytes;
        } catch (OutOfMemoryError oom) {
            throw new RuntimeException(oom);
        }
    }

    public void close() throws IOException {
        reader.close();
    }

    public short readShort() throws IOException {
        short s = reader.readShort();
        position += 2;
        return s;
    }

    public int position() {
        return position;
    }

    public void skipBytes(int messageSize) throws IOException {
        reader.skipBytes(messageSize);
        position += messageSize;
    }

    public long readLong() throws IOException {
        long l = reader.readLong();
        position += 8;
        return l;
    }
}
