/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.component.tags.ClientTagsCache;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.message.ClientIdentification;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * @author Marc Gathier
 */
@Component
public class CommandTargetValidator implements BiFunction<Command, ClientIdentification, Integer> {

    private final ClientTagsCache clientTagsCache;

    public CommandTargetValidator(ClientTagsCache clientTagsCache) {
        this.clientTagsCache = clientTagsCache;
    }


    @Override
    public Integer apply(Command command, ClientIdentification client) {
        if (command == null) {
            return 0;
        }
        Map<String, String> clientTags = clientTagsCache.apply(client);
        int score = 0;
        for (Map.Entry<String, String> tagEntry : clientTags.entrySet()) {
            score += match(tagEntry, command.getMetaDataMap());
        }

        return score;
    }

    private int match(Map.Entry<String, String> tagEntry, Map<String, MetaDataValue> metaDataMap) {
        MetaDataValue metaDataValue = metaDataMap.get(tagEntry.getKey());

        return metaDataValue == null ? 0 : matchStrings(tagEntry.getValue(), metaDataValue);
    }

    private int matchStrings(String value, MetaDataValue metaDataValue) {
        boolean match = false;
        switch (metaDataValue.getDataCase()) {
            case TEXT_VALUE:
                match = value.equals(metaDataValue.getTextValue());
                break;
            case NUMBER_VALUE:
                match = value.equals(String.valueOf(metaDataValue.getNumberValue()));
                break;
            case BOOLEAN_VALUE:
                match = value.equals(String.valueOf(metaDataValue.getBooleanValue()));
                break;
            case DOUBLE_VALUE:
                match = value.equals(String.valueOf(metaDataValue.getDoubleValue()));
                break;
            case BYTES_VALUE:
                break;
            case DATA_NOT_SET:
                break;
        }
        return match ? 1 : -1;
    }
}
