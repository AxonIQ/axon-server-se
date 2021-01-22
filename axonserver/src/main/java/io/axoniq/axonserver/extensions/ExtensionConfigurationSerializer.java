/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * Utility to serialize and deserialize extension configuration to strings.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class ExtensionConfigurationSerializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, Map<String, Object>> deserialize(String configuration) {
        if (configuration == null || configuration.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(configuration, Map.class);
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "Deserializing configuration failed: " + configuration,
                                                 ioException);
        }
    }

    public String serialize(Map<String, Map<String, Object>> configuration) {
        if (configuration == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(configuration);
        } catch (JsonProcessingException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Serializing configuration failed", e);
        }
    }
}
