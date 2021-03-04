package io.axoniq.axonserver.message;

import io.grpc.KnownLength;
import io.grpc.MethodDescriptor;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ByteArrayMarshaller implements MethodDescriptor.Marshaller<byte[]> {

    public static ByteArrayMarshaller instance() {
        return new ByteArrayMarshaller();
    }

    @Override
    public InputStream stream(byte[] bytes) {
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public byte[] parse(InputStream stream) {
        byte[] buf = null;
        try {
            if (stream instanceof KnownLength) {
                int size = stream.available();
                if (size > 0 && size <= 100) {
                    buf = new byte[size];

                    int remaining;
                    int position;
                    int count;
                    for (remaining = size; remaining > 0; remaining -= count) {
                        position = size - remaining;
                        count = stream.read(buf, position, remaining);
                        if (count == -1) {
                            break;
                        }
                    }

                    if (remaining != 0) {
                        position = size - remaining;
                        throw new RuntimeException("size inaccurate: " + size + " != " + position);
                    }

                } else if (size == 0) {
                    return new byte[0];
                }
            }
            if (buf == null) {
                buf = IOUtils.toByteArray(stream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buf;
    }
}
