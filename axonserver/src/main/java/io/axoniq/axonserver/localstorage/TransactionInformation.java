package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.localstorage.file.PositionKeepingDataInputStream;
import io.axoniq.axonserver.localstorage.file.VersionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Author: marc
 */
public class TransactionInformation {

    public static final int TRANSACTION_INFO_BYTES = 8;
    private final long index;
    private final byte version;

    public TransactionInformation(byte version, long index) {
        this.index = index;
        this.version = version;
    }

    public TransactionInformation(byte version, PositionKeepingDataInputStream reader) throws IOException {
            index = VersionUtils.hasIndexField(version) ? reader.readLong() : 0;
            this.version = version;
    }

    public TransactionInformation(byte version, ByteBuffer buffer) {
        index = VersionUtils.hasIndexField(version) ? buffer.getLong(): 0;
        this.version = version;
    }

    public long getIndex() {
        return index;
    }

    public byte getVersion() {
        return version;
    }

    public void writeTo(ByteBuffer writeBuffer) {
        writeBuffer.putLong(index);
    }
}
