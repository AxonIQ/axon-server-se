package io.axoniq.axonserver.enterprise.storage.jdbc;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.internal.MetaData;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marc Gathier
 */
public class ProtoMetaDataSerializer implements MetaDataSerializer {

    @Override
    public byte[] serialize(Map<String, MetaDataValue> metaData) {
        return MetaData.newBuilder().putAllMetaData(metaData).build().toByteArray();
    }

    @Override
    public Map<String, MetaDataValue> deserialize(byte[] metaData) {
        if( metaData == null || metaData.length == 0) {
            return new HashMap<>();
        }
        try {
            return MetaData.parseFrom(metaData).getMetaDataMap();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return new HashMap<>();
        }
    }
}
