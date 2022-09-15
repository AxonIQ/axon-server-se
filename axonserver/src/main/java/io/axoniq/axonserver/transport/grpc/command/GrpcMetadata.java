/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc.command;

import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.grpc.MetaDataValue;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GrpcMetadata implements Metadata {

    private final Map<String, MetaDataValue> metaDataMap;
    private final Map<String, Serializable> internalMetadata;

    public GrpcMetadata(Map<String, MetaDataValue> metaDataMap) {
        this(metaDataMap, Collections.emptyMap());
    }

    public GrpcMetadata(Map<String, MetaDataValue> metaDataMap, Map<String, Serializable> internalMetadata) {
        this.metaDataMap = metaDataMap;
        this.internalMetadata = internalMetadata;
    }

    @Override
    public Iterable<String> metadataKeys() {
        return Stream.concat(metaDataMap.keySet().stream(), internalMetadata.keySet().stream())
                     .collect(Collectors.toList());
    }

    @Override
    public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
        if (internalMetadata.containsKey(metadataKey)) {
            //noinspection unchecked
            return Optional.ofNullable((R) internalMetadata.get(metadataKey));
        }
        MetaDataValue value = metaDataMap.getOrDefault(metadataKey, MetaDataValue.getDefaultInstance());
        Serializable serializable = null;
        switch (value.getDataCase()) {
            case TEXT_VALUE:
                serializable = value.getTextValue();
                break;
            case NUMBER_VALUE:
                serializable = value.getNumberValue();
                break;
            case BOOLEAN_VALUE:
                serializable = value.getBooleanValue();
                break;
            case DOUBLE_VALUE:
                serializable = value.getDoubleValue();
                break;
            case BYTES_VALUE:
                serializable = value.getBytesValue();
                break;
            case DATA_NOT_SET:
                break;
        }
        //noinspection unchecked
        return Optional.ofNullable((R) serializable);
    }
}
