/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.grpc.MetaDataValue;

import java.util.Map;

/**
 * @author Marc Gathier
 */
public interface ProcessedEvent {

    int getSerializedSize();

    byte[] toByteArray();

    String getAggregateIdentifier();

    long getAggregateSequenceNumber();

    String getMessageIdentifier();

    byte[] getPayloadBytes();

    String getPayloadRevision();

    String getPayloadType();

    long getTimestamp();

    String getAggregateType();

    boolean isDomainEvent();

    Map<String, MetaDataValue> getMetaData();
}
