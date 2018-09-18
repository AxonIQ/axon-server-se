package io.axoniq.axonserver.message.event;

import io.grpc.MethodDescriptor;

import java.io.BufferedInputStream;
import java.io.InputStream;

/**
 * Author: marc
 */
public class InputStreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {

    public static InputStreamMarshaller inputStreamMarshaller() {
        return new InputStreamMarshaller();
    }

    @Override
    public InputStream stream(InputStream inputStream) {
        return inputStream;
    }

    @Override
    public InputStream parse(InputStream stream) {
        if (stream.markSupported()) {
            return stream;
        } else {
            return new BufferedInputStream(stream);
        }
    }

}
