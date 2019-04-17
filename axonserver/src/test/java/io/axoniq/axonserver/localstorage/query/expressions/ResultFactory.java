/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions;

import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.ListExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.MapExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NumericExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.StringExpressionResult;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class ResultFactory {

    private ResultFactory() {
    }

    public static ExpressionResult emptyMapValue() {
        return new MapExpressionResult(Collections.emptyMap());

    }
    public static ExpressionResult mapValue(String key, ExpressionResult value) {
        return new MapExpressionResult(Collections.singletonMap(key, value));
    }

    public static ExpressionResult mapValue(String key, ExpressionResult value, String key2, ExpressionResult value2) {
        Map<String, ExpressionResult> map = new HashMap<>();
        map.put(key, value);
        map.put(key2, value2);
        return new MapExpressionResult(map);
    }
    public static ExpressionResult mapValue(String key, ExpressionResult value, String key2, ExpressionResult value2, String key3, ExpressionResult value3) {
        Map<String, ExpressionResult> map = new HashMap<>();
        map.put(key, value);
        map.put(key2, value2);
        map.put(key3, value3);
        return new MapExpressionResult(map);
    }

    public static ExpressionResult nullValue() {
        return NullExpressionResult.INSTANCE;
    }

    public static ExpressionResult stringValue(String string) {
        return new StringExpressionResult(string);
    }

    public static ExpressionResult numericValue(long value) {
        return new NumericExpressionResult(value);
    }

    public static ExpressionResult listValue(ExpressionResult... values) {
        return new ListExpressionResult(values);
    }

}
