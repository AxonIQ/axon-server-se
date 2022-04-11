/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.util.SerializedObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marc Gathier
 */
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
                    put(key, SerializedObjectMapper.map(value.getBytesValue()));
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
