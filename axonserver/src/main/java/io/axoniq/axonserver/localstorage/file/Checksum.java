package io.axoniq.axonserver.localstorage.file;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Author: marc
 */
public class Checksum {
    private final CRC32 crc32;

    public Checksum() {
        crc32 = new CRC32();
        crc32.reset();
    }

    public int get() {
        return (int) crc32.getValue();
    }

    public Checksum update(byte[] bytes) {
        crc32.update(bytes);
        return this;
    }

    public Checksum update(ByteBuffer buffer, int position, int size) {
        byte[] bytes = new byte[size];
        ((ByteBuffer) buffer.duplicate().position(position)).get(bytes);
            crc32.update( bytes);
        return this;
    }

}
