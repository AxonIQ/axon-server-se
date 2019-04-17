/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.QueryResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DefaultQueryResult implements QueryResult {

    private final ExpressionResult value;
    private final boolean deleted;
    private final ListExpressionResult identifyingValues;
    private final ListExpressionResult sortValues;

    public DefaultQueryResult(ExpressionResult result) {
        this(result, false,
             ListExpressionResult.asListOrNull(result.getByIdentifier("token")),
             ListExpressionResult.asListOrNull(result.getByIdentifier("token")));
    }

    private DefaultQueryResult(ExpressionResult value, boolean deleted,
                               ListExpressionResult identifyingValues, ListExpressionResult sortValues) {
        this.value = value;

        this.deleted = deleted;
        this.identifyingValues = identifyingValues;
        this.sortValues = sortValues;
    }

    @Override
    public QueryResult deleted() {
        return new DefaultQueryResult(null, true, identifyingValues, null);
    }

    @Override
    public String asJson(ObjectMapper mapper) throws IOException {
        Map<String, Object> structure = new HashMap<>();
        if (deleted) {
            structure.put("deleted", true);
        }
        if (identifyingValues != null) {
            structure.put("idValues", identifyingValues);
        }
        if (sortValues != null) {
            structure.put("sortValues", sortValues);
        }
        if (value != null) {
            structure.put("value", getValue());
        }
        return mapper.writeValueAsString(structure);
    }

    @Override
    public ExpressionResult getValue() {
        return value;
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public ListExpressionResult getSortValues() {
        return sortValues;
    }

    @Override
    public ListExpressionResult getId() {
        return identifyingValues;
    }

    @Override
    public QueryResult withValue(ExpressionResult result) {
        return new DefaultQueryResult(result, deleted, identifyingValues, sortValues);
    }

    @Override
    public QueryResult withSortValues(ListExpressionResult sortValues) {
        return new DefaultQueryResult(value, deleted, identifyingValues, sortValues);
    }

    @Override
    public QueryResult withId(ListExpressionResult identifyingValues) {
        return new DefaultQueryResult(value, deleted, identifyingValues, sortValues);
    }
}
