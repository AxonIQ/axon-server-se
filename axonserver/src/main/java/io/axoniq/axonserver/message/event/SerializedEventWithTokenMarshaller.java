package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.grpc.MethodDescriptor;

import java.io.InputStream;

/**
 * Marshaller for {@link SerializedEventWithToken}.
 */
public class SerializedEventWithTokenMarshaller implements MethodDescriptor.Marshaller<SerializedEventWithToken> {

    @Override
    public InputStream stream(SerializedEventWithToken serializedEventWithToken) {
        return serializedEventWithToken.asInputStream();
    }

    @Override
    public SerializedEventWithToken parse(InputStream inputStream) {
        return new SerializedEventWithToken(inputStream);
    }
}
