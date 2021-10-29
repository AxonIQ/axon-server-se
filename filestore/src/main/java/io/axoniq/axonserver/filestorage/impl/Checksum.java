package io.axoniq.axonserver.filestorage.impl;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * @author Marc Gathier
 * @since 4.6.0
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
        for( int i = 0 ; i < size ; i++) {
            crc32.update( buffer.get(position+i));
        }
        return this;
    }

}
