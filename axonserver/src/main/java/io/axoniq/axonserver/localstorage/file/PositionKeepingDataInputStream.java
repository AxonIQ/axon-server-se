package io.axoniq.axonserver.localstorage.file;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Marc Gathier
 */
public class PositionKeepingDataInputStream {
    private int position = 0;
    private DataInputStream reader;

    public PositionKeepingDataInputStream(InputStream fileInputStream) {
        reader = new DataInputStream(fileInputStream);
    }

    public byte readByte() throws IOException {
        byte b  = reader.readByte();
        position++;
        return b;
    }

    public int readInt() throws IOException {
        int i = reader.readInt();
        position += 4;
        return i;
    }

    public void position(int position) throws IOException {
        if( position < this.position) throw new IOException("Cannot move backwards in datastream");

        reader.skipBytes(position - this.position);
        this.position = position;
    }

    public byte[] readEvent() throws IOException {
        int size = readInt();
        return readBytes(size);
    }

    private byte[] readBytes(int size) throws IOException {
        byte[] bytes = new byte[size];
        int total = reader.read(bytes);
        while( total < size ) {
            total += reader.read(bytes, total, size-total);
        }
        position  += size;
        return bytes;
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
}
