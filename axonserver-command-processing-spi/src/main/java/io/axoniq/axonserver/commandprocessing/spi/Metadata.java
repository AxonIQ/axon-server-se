/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.commandprocessing.spi;

import java.io.Serializable;
import java.util.Optional;

public interface Metadata extends Serializable {


    Iterable<String> metadataKeys();

    <R extends Serializable> Optional<R> metadataValue(String metadataKey);

    default <R extends Serializable> R metadataValue(String metadataKey, R defaultValue) {
        //noinspection unchecked
        return (R) metadataValue(metadataKey).orElse(defaultValue);
    }

    static boolean isInternal(String key) {
        return key.startsWith("__");
    }
}
