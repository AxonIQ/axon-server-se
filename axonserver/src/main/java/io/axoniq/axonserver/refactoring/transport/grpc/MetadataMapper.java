package io.axoniq.axonserver.refactoring.transport.grpc;

import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.refactoring.messaging.api.SerializedObject;
import io.axoniq.axonserver.refactoring.transport.Mapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Milan Savic
 */
@Component("metadataMapper")
public class MetadataMapper implements Mapper<Map<String, Object>, Map<String, MetaDataValue>> {

    private final Mapper<SerializedObject, io.axoniq.axonserver.grpc.SerializedObject> serializedObjectMapper;

    public MetadataMapper(
            @Qualifier("serializedObjectMapper")
            Mapper<SerializedObject, io.axoniq.axonserver.grpc.SerializedObject> serializedObjectMapper) {
        this.serializedObjectMapper = serializedObjectMapper;
    }

    @Override
    public Map<String, MetaDataValue> map(Map<String, Object> origin) {
        return origin.entrySet()
                     .stream()
                     .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                         Object value = e.getValue();
                         MetaDataValue.Builder builder = MetaDataValue.newBuilder();
                         if (value != null) {
                             if (value instanceof Boolean) {
                                 builder.setBooleanValue((Boolean) value);
                             } else if (value instanceof String) {
                                 builder.setTextValue(value.toString());
                             } else if (value instanceof Double) {
                                 builder.setDoubleValue((Double) value);
                             } else if (value instanceof Long) {
                                 builder.setNumberValue((Long) value);
                             } else if (value instanceof SerializedObject) {
                                 builder.setBytesValue(serializedObjectMapper.map((SerializedObject) value));
                             } else {
                                 throw new IllegalArgumentException("Unsupported metadata type: " + value.getClass());
                             }
                         }
                         return builder.build();
                     }));
    }

    @Override
    public Map<String, Object> unmap(Map<String, MetaDataValue> origin) {
        return null;
    }
}
