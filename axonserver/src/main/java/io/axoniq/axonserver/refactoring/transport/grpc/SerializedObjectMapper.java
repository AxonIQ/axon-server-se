package io.axoniq.axonserver.refactoring.transport.grpc;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.refactoring.messaging.api.SerializedObject;
import io.axoniq.axonserver.refactoring.transport.Mapper;
import org.springframework.stereotype.Component;

/**
 * @author Milan Savic
 */
@Component("serializedObjectMapper")
public class SerializedObjectMapper implements Mapper<SerializedObject, io.axoniq.axonserver.grpc.SerializedObject> {

    @Override
    public io.axoniq.axonserver.grpc.SerializedObject map(SerializedObject origin) {
        return io.axoniq.axonserver.grpc.SerializedObject.newBuilder()
                                                         .setType(origin.type())
                                                         .setRevision(origin.revision())
                                                         .setData(ByteString.copyFrom(origin.data()))
                                                         .build();
    }
}
