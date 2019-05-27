package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.grpc.MetaDataValue;

import java.util.Map;

/**
 * Serializer to serialize/deserialize event meta data.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public interface MetaDataSerializer {

    /**
     * Serializes the meta data map to a byte array.
     * @param metaData the meta data
     * @return serialized metadata
     */
    byte[] serialize(Map<String, MetaDataValue> metaData);

    /**
     * Deserializes the meta data from bytes to a map.
     * @param metaData serialized meta data
     * @return the meta data
     */
    Map<String, MetaDataValue> deserialize(byte[] metaData);
}
