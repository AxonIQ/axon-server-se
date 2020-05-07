/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.taskscheduler;

/**
 * Defines the interface for serializing/deserializing task payloads.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public interface TaskPayloadSerializer {

    /**
     * De-serializes the payload to a Java object. Throws an exception if there is a problem de-serializing the data.
     *
     * @param payload the serialized payload
     * @return the java object
     */
    Object deserialize(TaskPayload payload);

    /**
     * Serializes a Java object to a Payload object. Throws an exception if there is a problem serializing the data.
     *
     * @param object the java object
     * @return the serialized payload
     */
    TaskPayload serialize(Object object);
}
