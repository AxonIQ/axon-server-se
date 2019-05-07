package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.grpc.MetaDataValue;

import java.util.Map;

/**
 * @author Marc Gathier
 */
public interface MetaDataSerializer {

    byte[] serialize(Map<String, MetaDataValue> metaData);

    Map<String, MetaDataValue> deserialize(byte[] metaData);
}
