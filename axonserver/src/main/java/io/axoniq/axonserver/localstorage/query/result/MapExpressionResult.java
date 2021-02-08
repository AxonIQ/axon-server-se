/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.result;

import com.fasterxml.jackson.annotation.JsonValue;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class MapExpressionResult implements AbstractMapExpressionResult {
    private final Map<String, ExpressionResult> result;

    public MapExpressionResult(Map<String, ExpressionResult> result) {
        this.result = new HashMap<>(result);
    }

    @JsonValue
    @Override
    public Object getValue() {
        return result;
    }

    @Override
    public int compareTo(ExpressionResult o) {
        return 0;
    }

    @Override
    public ExpressionResult getByIdentifier(String identifier) {
        return result.get(identifier);
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean isNonNull() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapExpressionResult that = (MapExpressionResult) o;
        return Objects.equals(result, that.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(result);
    }

    @Override
    public Iterable<String> getColumnNames() {
        return result.keySet();
    }
}
