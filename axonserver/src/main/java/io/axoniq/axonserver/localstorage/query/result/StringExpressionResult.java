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
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class StringExpressionResult implements ExpressionResult {
    private final String value;

    public StringExpressionResult(String value) {
        this.value = value;
    }

    @Override
    public boolean isTrue() {
        return Boolean.valueOf(value);
    }

    @JsonValue
    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public boolean isNonNull() {
        return value != null;
    }

    @Override
    public int compareTo( ExpressionResult o) {
        if( value == null) return o.isNonNull() ? -1 : 0;
        if( o.getValue() == null) return 1;
        return value.compareTo(o.getValue().toString());
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public JSONObject asJson() {
        try {
            JSONParser p = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);
            return (JSONObject) p.parse(value);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Value is not valid JSON", e);
        }
    }

    @Override
    public ExpressionResult add(ExpressionResult other) {
        return new StringExpressionResult(value + other.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (! (o instanceof ExpressionResult)) return false;
        ExpressionResult that = (ExpressionResult) o;
        return Objects.equals(value, that.getValue().toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
