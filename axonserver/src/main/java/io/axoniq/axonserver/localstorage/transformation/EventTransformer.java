/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transformation;

/**
 * Transformer to transform events between protobuf event bytes and stored event bytes.
 * Transformer implementations may perform compression or encryption before storing.
 *
 * @author Marc Gathier
 */
public interface EventTransformer {

    /**
     * Converts stored bytes to protobuf bytes.
     *
     * @param eventBytes bytes as stored
     * @return protobuf event bytes
     */
    byte[] fromStorage(byte[] eventBytes);

    /**
     * Converts protobuf event bytes to bytes to store.
     *
     * @param bytes protobuf event bytes
     * @return transformed bytes to store
     */
    byte[] toStorage(byte[] bytes);
}
