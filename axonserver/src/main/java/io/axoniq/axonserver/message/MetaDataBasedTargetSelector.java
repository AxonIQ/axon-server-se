/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message;

import io.axoniq.axonserver.component.tags.ClientTagsCache;
import io.axoniq.axonserver.grpc.MetaDataValue;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Selects one or more target clients for a request based on the request's meta data. Finds the client with most
 * matching
 * tags. If multiple clients have same number of matching tags, all of them are returned. If there are no clients with
 * matching tags, all candidates are returned.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class MetaDataBasedTargetSelector
        implements
        BiFunction<Map<String, MetaDataValue>, Set<ClientStreamIdentification>, Set<ClientStreamIdentification>> {

    private final ClientTagsCache clientTagsCache;

    /**
     * Constructor
     *
     * @param clientTagsCache component containing the connected clients and their tags
     */
    public MetaDataBasedTargetSelector(ClientTagsCache clientTagsCache) {
        this.clientTagsCache = clientTagsCache;
    }

    /**
     * Finds the candidates that best match the meta data from the list of provided candidates.
     *
     * @param metaDataMap meta data from the request
     * @param candidates  set of clients capable of handling the request
     * @return subset of candidates with best matching tags
     */
    @Override
    public Set<ClientStreamIdentification> apply(Map<String, MetaDataValue> metaDataMap,
                                                 Set<ClientStreamIdentification> candidates) {
        if (candidates.size() < 2 || metaDataMap.isEmpty()) {
            return candidates;
        }
        Map<ClientStreamIdentification, Integer> scorePerClient = new HashMap<>();
        candidates.forEach(candidate -> scorePerClient.computeIfAbsent(candidate,
                                                                       m -> score(metaDataMap, m)));

        return getHighestScore(scorePerClient);
    }

    private int score(Map<String, MetaDataValue> metaDataMap, ClientStreamIdentification client) {
        if (metaDataMap.isEmpty()) {
            return 0;
        }
        Map<String, String> clientTags = clientTagsCache.apply(client);
        int score = 0;
        for (Map.Entry<String, String> tagEntry : clientTags.entrySet()) {
            score += match(tagEntry, metaDataMap);
        }

        return score;
    }

    private int match(Map.Entry<String, String> tagEntry, Map<String, MetaDataValue> metaDataMap) {
        MetaDataValue metaDataValue = metaDataMap.get(tagEntry.getKey());

        return metaDataValue == null ? 0 : matchValues(tagEntry.getValue(), metaDataValue);
    }

    private int matchValues(String value, MetaDataValue metaDataValue) {
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
            default:
                break;
        }
        return match ? 1 : -1;
    }

    private Set<ClientStreamIdentification> getHighestScore(Map<ClientStreamIdentification, Integer> scorePerClient) {
        Set<ClientStreamIdentification> bestClients = new HashSet<>();
        int highest = Integer.MIN_VALUE;
        for (Map.Entry<ClientStreamIdentification, Integer> score : scorePerClient.entrySet()) {
            if (score.getValue() > highest) {
                bestClients.clear();
                highest = score.getValue();
            }
            if (score.getValue() == highest) {
                bestClients.add(score.getKey());
            }
        }
        return bestClients;
    }
}
