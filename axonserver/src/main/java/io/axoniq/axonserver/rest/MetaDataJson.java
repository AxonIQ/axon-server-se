package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.platform.KeepNames;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: marc
 */
@KeepNames
public class MetaDataJson extends HashMap<String,Object> {

    public MetaDataJson() {
        super();
    }

    public MetaDataJson(Map<String, MetaDataValue> metaDataMap) {
        super();

        metaDataMap.forEach((key, value) -> {
            switch (value.getDataCase()) {
                case TEXT_VALUE:
                    put(key, value.getTextValue());
                    break;
                case NUMBER_VALUE:
                    put(key, value.getNumberValue());
                    break;
                case BOOLEAN_VALUE:
                    put(key, value.getBooleanValue());
                    break;
                case DOUBLE_VALUE:
                    put(key, value.getDoubleValue());
                    break;
                case BYTES_VALUE:
                    put(key, value.getBytesValue());
                    break;
                case DATA_NOT_SET:
                    break;
            }
        });

    }

    public Map<String, MetaDataValue> asMetaDataValueMap() {
        Map<String, MetaDataValue> map = new HashMap<>();
        this.forEach((key, value) -> {
            if( value instanceof String) {
                map.put(key, MetaDataValue.newBuilder().setTextValue((String)value).build());
            } else if( value instanceof Double || value instanceof Float) {
                map.put(key, MetaDataValue.newBuilder().setDoubleValue(((Number) value).doubleValue()).build());
            } else if( value instanceof Number) {
                map.put(key, MetaDataValue.newBuilder().setNumberValue(((Number) value).longValue()).build());
            } else if( value instanceof Boolean) {
                map.put(key, MetaDataValue.newBuilder().setBooleanValue((Boolean) value).build());
            }
        });

        return map;
    }
}
